"""
Module that contains the classes for downloading files from FTP or HTTP
These classes are designed to be used in the :class:`.Downloader` class
"""

import os
import time
from datetime import datetime
from typing import Optional, Union
from pathlib import Path
import logging
from functools import partial
import ssl
import ftplib
from urllib import request, parse, error

from .enums import ConnectionType, DownloadMode

# from dateutil import parser


class FileDownloader:
    """FTP helper class to download file preserving timestamp and to get file info, among others"""

    LOGGER = logging.getLogger(__name__)

    def __init__(
        self,
        server: str,
        connection_type: Union[ConnectionType, str] = ConnectionType.HTTP,
        download_mode: Union[DownloadMode, str] = DownloadMode.UPDATE,
        log_level: int = logging.INFO,
        check_interval: int = 28800,
    ):
        """
        Initialize a FileDownloader instance

        Args:
            url (str): URL to the server.
            connection_type (Union[str, ConnectionType], optional): Type of connection.
            Defaults to "http".
            download_mode (Union[DownloadMode, str], optional): Download mode behavior.
            Defaults to UPDATE.
            log_level (int, optional): Logging level. Defaults to logging.INFO.
            check_interval (int, optional): Time in seconds to cache file check results.
            Defaults to 28800 (8 hours). Set to 0 to disable caching.

        """
        if connection_type == ConnectionType.FTP:
            raise NotImplementedError("FTP connection is not yet implemented")

        # save the url and type of connection
        self._server = server
        self._connection_type = connection_type
        self._download_mode = download_mode
        self._check_interval = check_interval
        self._check_cache = {}  # {remote_url: last_check_timestamp}

        # if it is an FTP connection, the ftp and context variables will be set up
        if self._connection_type == ConnectionType.FTP:
            self._context = ssl._create_unverified_context()  # pylint: disable=W0212
            self._ftp = FileDownloader.open_ftp_connection(server)

        # print the representation of the object
        self.logger = FileDownloader._setup_logger(log_level)
        self.logger.info(self.__repr__())

    # -------------------- Logging Functionality --------------------
    @staticmethod
    def _setup_logger(log_level: int) -> logging.Logger:
        """Set up the logger"""
        FileDownloader.LOGGER.handlers.clear()
        FileDownloader.LOGGER.setLevel(log_level)
        handler = logging.StreamHandler()
        FileDownloader.LOGGER.addHandler(handler)
        return FileDownloader.LOGGER

    # -------------------- FTP Connection Functionality --------------------
    @staticmethod
    def open_ftp_connection(
        server: str, retrials: int = 5, logger: Optional[logging.Logger] = None
    ) -> ftplib.FTP:
        """Open an ftp connection and return an FTP instance"""
        for attempt in range(retrials):
            try:
                ftp = ftplib.FTP(server)
                ftp.login()
                ftp.sendcmd("TYPE I")
                return ftp

            except Exception as exc:  # pylint: disable=broad-except
                msg = f"Attempt {attempt + 1} to connect failed. "
                msg += f"Exception {type(exc)}: {exc}"

                if logger is not None:
                    logger.error(msg)
                else:
                    print(msg)

        raise ConnectionError(f"Connection to {server} could not be estabilished")

    def get_connection(self, alt_server: Optional[str] = None) -> Optional[ftplib.FTP]:
        """
        Return a connection. If current connection is closed, connect again.
        If an alternative server is provided, return the alternative server.
        """
        if alt_server is not None:
            return FileDownloader.open_ftp_connection(alt_server)

        if not self.is_connected:
            self._ftp = FileDownloader.open_ftp_connection(self._server)

        return self._ftp

    # -------------------- Utility Properties --------------------
    @property
    def server_url(self) -> str:
        """Return the url of the server with correct scheme (http or ftp)"""

        parsed_url = parse.urlparse(self._server)
        scheme = "ftp" if self._connection_type == ConnectionType.FTP else "http"
        parsed_url = parsed_url._replace(scheme=scheme, netloc=self._server, path="")

        return parsed_url.geturl()

    @property
    def is_connected(self) -> bool:
        """Check if the connection is open or if server is accessible"""

        if self._connection_type == ConnectionType.FTP:
            try:
                # test if the ftp is still responding
                self._ftp.pwd()
                return True

            except Exception:  # pylint:disable=broad-except
                # otherwise, return False
                return False
        else:
            # try to reach the url through a http request
            try:
                with request.urlopen(self.server_url):
                    pass

            except Exception:  # pylint:disable=broad-except
                return False

            return True

    # -------------------- Private Methods --------------------
    def _download_http_file(self, remote_file: str, local_path: Path):
        """
        Download an http file preserving filename and timestamps.
        The behavior of this function will depend on the DownloadMode specified.
        If the DownloadMode is FORCE, it will always download the file, regardless if it already exists
        IF the DownloadMode is NO_UPDATE, it will not download if the file already exists
        If the DownloadMode is UPDATE, it will only download if the file does not already exists or if it has been modified
        """

        # first, let's check if the local file already exists
        # If it exists and mode is NO_UPDATE, just skip
        if local_path.exists() and self._download_mode == DownloadMode.NO_UPDATE:
            self.logger.debug("Skipping %s. File already exists", local_path.name)
            return

        # For UPDATE mode with existing file, check timestamps BEFORE downloading
        if local_path.exists() and self._download_mode == DownloadMode.UPDATE:
            # Check if we recently verified this file (cache check)
            cache_key = remote_file

            if self._check_interval > 0 and cache_key in self._check_cache:
                time_since_check = time.time() - self._check_cache[cache_key]
                if time_since_check < self._check_interval:
                    # Recently verified, skip HEAD request
                    self.logger.debug(
                        "Skipping check for %s (verified %.1f hours ago)",
                        local_path.name,
                        time_since_check / 3600,
                    )
                    return  # Early exit - no HEAD request needed

            # Cache miss/expired - make a HEAD request to get only headers, not the file content
            req = request.Request(remote_file, method="HEAD")
            try:
                with request.urlopen(req) as response:
                    remote_dt_str = response.headers.get("Last-Modified")
                    if remote_dt_str:
                        date_format = "%a, %d %b %Y %H:%M:%S %Z"
                        remote_dt = datetime.strptime(remote_dt_str, date_format)
                        remote_mtime = time.mktime(remote_dt.timetuple())
                        local_mtime = local_path.stat().st_mtime

                        # if dates are the same, just skip because file already updated
                        if local_mtime == remote_mtime:
                            # Cache this successful check
                            if self._check_interval > 0:
                                self._check_cache[cache_key] = time.time()
                            self.logger.debug(
                                "Skipping download of %s. File already updated",
                                local_path.name,
                            )
                            return
                        else:
                            self.logger.debug(
                                "Downloading %s. File has been modified",
                                local_path.name,
                            )
            except error.HTTPError:
                # If HEAD request fails, proceed with normal download
                self.logger.debug("HEAD request failed, proceeding with full download")

        # Now download the file (for FORCE, UPDATE with changes, or new files)
        with request.urlopen(remote_file) as response:
            remote_dt_str = response.headers.get("Last-Modified")
            remote_mtime = None
            if remote_dt_str:
                date_format = "%a, %d %b %Y %H:%M:%S %Z"
                remote_dt = datetime.strptime(remote_dt_str, date_format)
                remote_mtime = time.mktime(remote_dt.timetuple())

            if not local_path.exists():
                self.logger.debug(
                    "Downloading %s. File does not exist", local_path.name
                )
            elif self._download_mode == DownloadMode.FORCE:
                self.logger.debug(
                    "Downloading %s. File already exists and mode is FORCE",
                    local_path.name,
                )

            # Download the file
            with open(local_path, "wb") as out_file:
                data = response.read()
                out_file.write(data)

            # Update the modification time if available
            if remote_mtime is not None:
                os.utime(local_path, (remote_mtime, remote_mtime))

    @staticmethod
    def _download_ftp_file(
        ftp: ftplib.FTP,
        remote_file: str,
        local_path: Path,
    ):
        """Download an ftp file preserving filename and timestamps"""
        # get the filename and set the target path
        with open(local_path, "wb") as local_file:
            ftp.retrbinary("RETR " + remote_file, local_file.write)

    # -------------------- Public Methods --------------------
    def clear_check_cache(self) -> None:
        """
        Clear the check cache to force fresh checks on next download.
        Useful when you know files have been updated externally.
        """
        self._check_cache.clear()
        self.logger.debug("Check cache cleared")

    def download_file(
        self,
        remote_file: Union[str, Path],
        local_folder: Union[str, Path],
        retrials: int = 5,
    ) -> Optional[Path]:
        """
        Download an ftp file preserving filename and timestamps.
        In the specific case the file does not exists in the server (error 404), we return None.
        """

        # Specify the download function according to the connection type
        if self._connection_type == ConnectionType.HTTP:
            download_fn = self._download_http_file
        else:
            ftp = self.get_connection()
            if ftp is None:
                raise ConnectionError("FTP connection could not be established")
            download_fn = partial(self._download_ftp_file, ftp=ftp)

        # get the filename and set the local path
        filename = os.path.basename(remote_file)
        local_path = Path(local_folder) / filename
        remote_file = self.server_url + str(remote_file)

        for attempt in range(retrials):
            try:
                if attempt > 0:
                    self.logger.error("Retrying - Attempt=%d", attempt)

                download_fn(remote_file, local_path)

                break

            except EOFError as e:
                self.logger.error("File %r was not downloaded correctly.", filename)
                self.logger.error(e)

            except error.HTTPError as e:
                # if the error code is 404, we know that the file does not exists
                if e.code == 404:
                    self.logger.warning("File %r was not available.", filename)
                    return None

            except Exception as e:  # pylint: disable=broad-except
                self.logger.error(e)

            finally:
                if attempt == retrials - 1:
                    raise ConnectionError(f"Not possible to download {remote_file}")

        return local_path

    # -------------------- Dundler Methods --------------------
    def __repr__(self) -> str:
        if self._connection_type == ConnectionType.HTTP:
            output = f"Using wget through HTTP on: {self._server}"
        else:
            output = f"FTP {'' if self.is_connected else 'Not '}\n"
            output += f"connected to server {self._ftp.host}"

        return output


__all__ = ["FileDownloader"]
