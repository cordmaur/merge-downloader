"""
Test Utils Module
"""

from pathlib import Path
from datetime import datetime
from socket import gaierror
import ftplib
import pytest
from mergedownloader.utils import OSUtil
from mergedownloader.inpeparser import INPEParsers


class TestOSUtil:
    """Test the OSUTil class"""

    def test_get_local_file_info(self):
        """Test the get_local_file_info method"""
        file_info = OSUtil.get_local_file_info(
            "./tests/data/DAILY_RAIN/MERGE_CPTEC_20230301.tif"
        )

        assert isinstance(file_info["datetime"], datetime)
        assert isinstance(file_info["size"], int)
