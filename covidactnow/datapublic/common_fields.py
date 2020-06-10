"""
Data schema shared between code in covid-data-public and covid-data-model repos.
"""
from enum import Enum


class GetByValueMixin:
    """Mixin making it easy to get an Enum object or None if not found.

    Unlike `YourEnumClass(value)`, the `get` method does not raise `ValueError` when `value`
    is not in the enum.
    """

    @classmethod
    def get(cls, value):
        return cls._value2member_map_.get(value, None)


class ValueAsStrMixin:
    def __str__(self):
        return self.value


class CommonFields(GetByValueMixin, ValueAsStrMixin, str, Enum):
    """Common field names shared across different sources of data"""

    FIPS = "fips"

    DATE = "date"

    # 2 letter state abbreviation, i.e. MA
    STATE = "state"

    COUNTRY = "country"

    COUNTY = "county"

    AGGREGATE_LEVEL = "aggregate_level"

    # Full state name, i.e. Massachusetts
    STATE_FULL_NAME = "state_full_name"

    CASES = "cases"
    DEATHS = "deaths"
    RECOVERED = "recovered"
    CUMULATIVE_HOSPITALIZED = "cumulative_hospitalized"
    CUMULATIVE_ICU = "cumulative_icu"

    POSITIVE_TESTS = "positive_tests"
    NEGATIVE_TESTS = "negative_tests"

    # Current values
    CURRENT_ICU = "current_icu"
    CURRENT_HOSPITALIZED = "current_hospitalized"
    CURRENT_VENTILATED = "current_ventilated"

    POPULATION = "population"

    STAFFED_BEDS = "staffed_beds"
    LICENSED_BEDS = "licensed_beds"
    ICU_BEDS = "icu_beds"
    ALL_BED_TYPICAL_OCCUPANCY_RATE = "all_beds_occupancy_rate"
    ICU_TYPICAL_OCCUPANCY_RATE = "icu_occupancy_rate"
    MAX_BED_COUNT = "max_bed_count"
    VENTILATOR_CAPACITY = "ventilator_capacity"

    CURRENT_HOSPITALIZED_TOTAL = "current_hospitalized_total"
    CURRENT_ICU_TOTAL = "current_icu_total"

    CONTACT_TRACERS_COUNT = "contact_tracers_count"


COMMON_FIELDS_TIMESERIES_KEYS = [CommonFields.FIPS, CommonFields.DATE]


COMMON_FIELDS_ORDER_MAP = {common: i for i, common in enumerate(CommonFields)}
