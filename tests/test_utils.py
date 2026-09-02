"""
Test Utils Module
"""

from datetime import datetime
from mergedownloader.utils import OSUtil


class TestOSUtil:
    """Test the OSUTil class"""

    def test_get_local_file_info(self):
        """Test the get_local_file_info method"""
        file_info = OSUtil.get_local_file_info(
            "./tests/data/DAILY/2026/08/MERGE_CPTEC_20260801.grib2"
        )

        assert isinstance(file_info["datetime"], datetime)
        assert isinstance(file_info["size"], int)
