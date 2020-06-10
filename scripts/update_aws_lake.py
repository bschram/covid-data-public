import pathlib
import shutil
import sys
from collections import defaultdict
from enum import Enum
from typing import Union, Optional, List, Dict, Any, Iterable, Tuple, Set

import boto3
import botocore
import botocore.client
import click
import pandas as pd

import structlog
from pydantic import BaseModel, DirectoryPath
from structlog._config import BoundLoggerLazyProxy
from structlog.threadlocal import tmp_bind

from covidactnow.datapublic import common_init
from covidactnow.datapublic.common_df import write_df_as_csv, sort_common_field_columns
from covidactnow.datapublic.common_fields import (
    GetByValueMixin,
    CommonFields,
    COMMON_FIELDS_TIMESERIES_KEYS,
    COMMON_LEGACY_REGION_FIELDS,
)
from scripts.update_covid_data_scraper import (
    FieldNameAndCommonField,
    load_county_fips_data,
)
from scripts.update_test_and_trace import load_census_state

DATA_ROOT = pathlib.Path(__file__).parent.parent / "data"


class Fields(GetByValueMixin, FieldNameAndCommonField, Enum):
    TIME_VALUE = "time_value", CommonFields.DATE
    TIME_TYPE = "time_type", None
    GEO_VALUE = "geo_value", None
    GEO_TYPE = "geo_type", None
    SIGNAL = "signal", None
    VALUE = "value", None


KNOWN_IGNORED_FIELDS = {
    "input_path",
    "data_source",
    "direction",
    "stderr",
    "sample_size",
    "filename",
}
ALL_KNOWN_FIELDS = set(Fields) | KNOWN_IGNORED_FIELDS


DELPHI_BUCKET_NAME = "covid19-lake"
COVIDCAST_PREFIX = "covidcast/json"


def _get_unsigned_s3_client():
    return boto3.client("s3", config=botocore.client.Config(signature_version=botocore.UNSIGNED))


def _group_covidcast_files_by_source(s3_keys: List[str]) -> Dict[str, List[str]]:
    """
    The delphi public s3 bucket contains files with paths that look like this,
    'covidcast/json/data/consensus/part-00000-64b3ef4a-f21d-4ff8-8993-80e9447b3e42-c000.json'

    Given an array of keys, this function groups all the file parts for a given data type/data source
    for further processing.

    There is an additional metadata file which is not included.

    """
    files_by_type = defaultdict(list)
    for path in s3_keys:
        data_type = path.split("/")[3]
        if data_type != "metadata.json":
            files_by_type[data_type].append(path)
    return files_by_type


def _get_delphi_covidcast_metadata(bucket_name: str = DELPHI_BUCKET_NAME) -> pd.DataFrame:
    """Fetch metadata. Not used in normal update job."""
    covidcast_metadata_path = "s3://" + bucket_name + "/" + "covidcast/json/metadata/metadata.json"
    metadata_df = pd.read_json(covidcast_metadata_path, lines=True)

    metadata_df.min_time = pd.to_datetime(metadata_df.min_time, format="%Y%m%d")
    metadata_df.max_time = pd.to_datetime(metadata_df.max_time, format="%Y%m%d")
    metadata_df.last_update = pd.to_datetime(metadata_df.last_update, unit="s")
    return metadata_df


class AwsDataLakeCopier(BaseModel):
    local_mirror_dir: DirectoryPath

    # An unsigned s3 client
    s3: Any

    log: Union[structlog.BoundLoggerBase, BoundLoggerLazyProxy]

    class Config:
        arbitrary_types_allowed = True

    @staticmethod
    def make_with_data_root(data_root: pathlib.Path) -> "AwsDataLakeCopier":
        return AwsDataLakeCopier(
            local_mirror_dir=data_root / "aws-lake" / "mirror",
            s3=_get_unsigned_s3_client(),
            log=structlog.get_logger(),
        )

    def _get_latest_delphi_files(
        self, bucket_name: str = DELPHI_BUCKET_NAME, prefix: Optional[str] = COVIDCAST_PREFIX
    ) -> List[str]:
        """
        Given an s3 bucket name and optional path prefix, fetch all file names matching that prefix.
        """
        paginator = self.s3.get_paginator("list_objects")
        # The bucket has a ton of stuff and depending on the prefix value you
        # choose you may exceed the max list_objects return size (1000).
        # The paginator allows you always fetch all of the file paths.
        page_iterator = paginator.paginate(Bucket=bucket_name, Prefix=prefix)
        s3_keys = []
        for page in page_iterator:
            s3_keys.extend(x["Key"] for x in page["Contents"])
        return s3_keys

    def _cache_data_locally(
        self, s3_keys: List[str], source_dir: DirectoryPath, bucket_name=DELPHI_BUCKET_NAME
    ) -> None:
        """Download json files from s3"""
        source_dir.mkdir(parents=True)

        s3 = _get_unsigned_s3_client()
        for key in s3_keys:
            self.log.info(f"Downloading file s3://{bucket_name}/{key}")
            filename = pathlib.Path(key).name
            local_file = source_dir / filename
            s3.download_file(bucket_name, key, str(local_file))

    def replace_local_mirror(self):
        keys = self._get_latest_delphi_files()
        files_by_source = _group_covidcast_files_by_source(keys)
        self.log.info("Removing existing local mirror directory", mirror_dir=self.local_mirror_dir)
        shutil.rmtree(self.local_mirror_dir)

        for data_source, keys in files_by_source.items():
            if not data_source.startswith("jhu"):
                self.log.info(f"Caching {len(keys)} {data_source} files locally.")
                self._cache_data_locally(keys, self.local_mirror_dir / data_source)
        self.log.info(
            "Finished download to local mirror directory", mirror_dir=self.local_mirror_dir
        )

    def get_sources(self) -> Iterable[Tuple[str, Iterable[pathlib.Path]]]:
        """Iterate through local mirror source names with the paths of JSON files"""
        for dir in [x for x in self.local_mirror_dir.iterdir() if x.is_dir()]:
            yield dir.name, dir.glob("*.json")


class AwsDataLakeTransformer(BaseModel):
    # A DataFrame with rows containing CommonFields values for regions (`fips`, `county`, ...) indexed by
    # `geo_value` and `geo_type`. This is used to merge the CommonFields values into the combined_df for both
    # states and counties.
    geo_fields_to_common_fields: pd.DataFrame

    class Config:
        arbitrary_types_allowed = True

    @staticmethod
    def make_with_data_root(data_root: pathlib.Path) -> "AwsDataLakeTransformer":
        county_geos = load_county_fips_data(data_root / "misc" / "fips_population.csv")
        county_geos[Fields.GEO_VALUE] = pd.to_numeric(county_geos[CommonFields.FIPS])
        county_geos[Fields.GEO_TYPE] = "county"
        county_geos[CommonFields.AGGREGATE_LEVEL] = "county"
        county_geos[CommonFields.COUNTRY] = "USA"

        state_geos = load_census_state(data_root / "misc" / "state.txt")
        state_geos[Fields.GEO_VALUE] = state_geos[CommonFields.STATE].str.lower()
        state_geos[Fields.GEO_TYPE] = "state"
        state_geos[CommonFields.AGGREGATE_LEVEL] = "state"
        state_geos[CommonFields.COUNTRY] = "USA"

        all_geos = pd.concat([county_geos, state_geos])

        return AwsDataLakeTransformer(
            geo_fields_to_common_fields=all_geos.set_index([Fields.GEO_TYPE, Fields.GEO_VALUE])[
                COMMON_LEGACY_REGION_FIELDS
            ]
        )

    def _load_json_lines(self, log, source_files: Iterable[pathlib.Path]):
        combined_df = pd.DataFrame()
        for f in source_files:
            part_df = pd.read_json(f, lines=True)
            part_df[Fields.TIME_VALUE] = pd.to_datetime(part_df[Fields.TIME_VALUE], format="%Y%m%d")
            combined_df = combined_df.append(part_df, ignore_index=True)
        unknown_fields = set(combined_df.columns) - ALL_KNOWN_FIELDS
        if unknown_fields:
            log.warning(
                "Found unknown fields. Did the structure of the JSON change?",
                unknown_fields=unknown_fields,
            )
        # Keep only columns that appear in the Fields enum.
        combined_df = combined_df.loc[:, list(Fields)]
        return combined_df

    def _map_columns(self, df: pd.DataFrame, log: structlog.BoundLoggerBase) -> pd.DataFrame:
        """Given a DataFrame with `Fields` columns, return county data with `CommonFields` columns."""
        df = df.merge(
            self.geo_fields_to_common_fields,
            left_on=[Fields.GEO_TYPE, Fields.GEO_VALUE],
            suffixes=(False, False),
            how="left",
            right_index=True,
        )
        no_match_mask = df[CommonFields.FIPS].isna()
        if no_match_mask.sum() > 0:
            log.warning(
                "Dropping rows that did not merge by geo_value",
                geo_value_count=df.loc[no_match_mask].groupby(Fields.GEO_VALUE).size().to_dict(),
            )
            df = df.loc[~no_match_mask, :]
        df = df.drop(
            columns=[f for f in Fields if f in df.columns and f.common_field is None]
        ).rename(columns={f: f.common_field for f in Fields if f.common_field is not None})
        return df

    def _make_column_per_signal(self, combined_df, log):
        grouped = combined_df.groupby(
            [Fields.GEO_TYPE, Fields.GEO_VALUE, Fields.TIME_TYPE, Fields.TIME_VALUE, Fields.SIGNAL]
        )
        group_sizes = grouped.size()
        if (group_sizes > 1).any():
            log.warning(
                "Found duplicate values",
                count=(group_sizes > 1).sum(),
                rows=grouped[group_sizes > 1],
            )
        # Proceed as though each group in `grouped` contains a single row. last() turns the groups into a DataFrame
        # and unstack moves values in the last row index, Fields.SIGNAL, to the column index.
        unstacked_df = grouped.last().unstack()
        # Remove the top level *column* index `signal` leaving the values from signal as the column names.
        unstacked_df.columns = unstacked_df.columns.droplevel()
        # Restore the row indexes created by the groupby to be regular columns.
        unstacked_df = unstacked_df.reset_index()
        return unstacked_df

    def transform(self, source_files: Iterable[pathlib.Path], log) -> pd.DataFrame:
        combined_df = self._load_json_lines(log, source_files)

        # Only keep rows for region types that can be processed by CAN.
        acceptable_geo_type = {"state", "county"}
        can_regions_df = combined_df.loc[combined_df[Fields.GEO_TYPE].isin(acceptable_geo_type), :]

        unstacked_df = self._make_column_per_signal(can_regions_df, log)

        output_df = self._map_columns(unstacked_df, log).set_index(COMMON_FIELDS_TIMESERIES_KEYS)

        log.info(
            "Loaded dataframe",
            input_rows=len(combined_df),
            input_by_geo_types=combined_df.groupby(Fields.GEO_TYPE).size().to_dict(),
            input_signals=list(combined_df[Fields.SIGNAL].unique()),
            output_rows=len(output_df),
            output_by_agg_level=output_df.groupby(CommonFields.AGGREGATE_LEVEL).size().to_dict(),
        )
        return output_df


@click.command()
@click.option("--replace_local_mirror", is_flag=True)
def main(replace_local_mirror: bool):
    common_init.configure_structlog()

    copier = AwsDataLakeCopier.make_with_data_root(DATA_ROOT)
    if replace_local_mirror:
        copier.replace_local_mirror()

    transformer = AwsDataLakeTransformer.make_with_data_root(DATA_ROOT)
    for source_name, source_files in copier.get_sources():
        log = structlog.get_logger(source_name=source_name)
        write_df_as_csv(
            transformer.transform(source_files, log),
            DATA_ROOT / "aws-lake" / f"timeseries-{source_name}.csv",
            log,
        )


if __name__ == "__main__":
    main()  # pylint: disable=no-value-for-parameter
