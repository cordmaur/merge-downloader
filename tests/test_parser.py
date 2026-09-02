"""Tests for parser path and URL construction."""

from datetime import datetime

from mergedownloader.enums import DateFrequency
from mergedownloader.parser import DownloaderParser


class SampleDownloaderParser(DownloaderParser):
    """Minimal parser used to verify the remote URL contract."""

    constants = {
        "root": "https://example.com/data/",
        "var": "rain",
        "name": "Rain",
        "freq": DateFrequency.DAILY,
        "post_proc": None,
    }

    def filename(self, date: datetime, **_) -> str:
        return f"rain-{date:%Y%m%d}.nc"

    def foldername(self, date: datetime, **__) -> str:
        return f"daily/{date:%Y/%m}"


def test_remote_folder_uses_parser_http_root():
    parser = SampleDownloaderParser()

    assert parser.remote_folder(datetime(2022, 1, 1)) == (
        "https://example.com/data/daily/2022/01"
    )


def test_remote_target_is_a_complete_http_url():
    parser = SampleDownloaderParser()

    assert parser.remote_target(datetime(2022, 1, 1)) == (
        "https://example.com/data/daily/2022/01/rain-20220101.nc"
    )
