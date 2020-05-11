import datetime
import logging
import os
from urllib.request import urlopen
from zipfile import ZipFile
import pytz

_logger = logging.getLogger(__name__)


class DatasetUpdaterBase:
    @staticmethod
    def _stamp() -> str:
        """
        String of the current date and time.
        So that we're consistent about how we mark these
        """
        pacific = pytz.timezone('US/Pacific')
        d = datetime.datetime.now(pacific)
        return d.strftime('%A %b %d %I:%M:%S %p %Z')

    
    def clone_repo_to_dir(self, url, _dir):
        with open(os.path.join(_dir, 'temp.zip'), 'wb') as zip:
            zip.write(urlopen(url).read())
        with ZipFile(os.path.join(_dir, 'temp.zip')) as zf:
            zf.extractall(path=os.path.join(_dir))
        return _dir
