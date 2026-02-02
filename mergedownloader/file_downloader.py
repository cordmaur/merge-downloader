"""
Module that contains the classes for downloading files from FTP or HTTP
These classes are designed to be used in the :class:`.Downloader` class
"""

import os
import time
import tempfile
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
    def _update_check_cache(self, cache_key: str) -> None:
        """Update the check cache with current timestamp"""
        if self._check_interval > 0:
            self._check_cache[cache_key] = time.time()
            self.logger.debug("Cache updated for %s", cache_key)

    def _should_skip_download(
        self, local_path: Path, remote_file: str
    ) -> tuple[bool, str]:
        """
        Determine if download should be skipped based on mode and cache.
        Returns (should_skip, reason) tuple.
        """
        # NO_UPDATE mode: skip if file exists
        if local_path.exists() and self._download_mode == DownloadMode.NO_UPDATE:
            self._update_check_cache(remote_file)
            return True, "File already exists (NO_UPDATE mode)"

        # File doesn't exist: must download
        if not local_path.exists():
            return False, "File does not exist"

        # FORCE mode: always download
        if self._download_mode == DownloadMode.FORCE:
            return False, "FORCE mode enabled"

        # UPDATE mode with existing file: check cache first
        if self._check_interval > 0 and remote_file in self._check_cache:
            time_since_check = time.time() - self._check_cache[remote_file]
            if time_since_check < self._check_interval:
                return (
                    True,
                    f"Recently verified ({time_since_check / 3600:.1f} hours ago)",
                )

        return False, "UPDATE mode - need to check remote"

    def _check_remote_modified(
        self, remote_file: str, local_path: Path
    ) -> tuple[bool, Optional[float]]:
        """
        Check if remote file has been modified compared to local file.
        Returns (is_modified, remote_mtime) tuple.
        remote_mtime is None if cannot be determined.
        """
        try:
            req = request.Request(remote_file, method="HEAD")
            with request.urlopen(req) as response:
                remote_dt_str = response.headers.get("Last-Modified")
                if not remote_dt_str:
                    # Cannot determine remote time, assume modified
                    self.logger.debug("No Last-Modified header, assuming file modified")
                    return True, None

                date_format = "%a, %d %b %Y %H:%M:%S %Z"
                remote_dt = datetime.strptime(remote_dt_str, date_format)
                remote_mtime = time.mktime(remote_dt.timetuple())
                local_mtime = local_path.stat().st_mtime

                self.logger.debug(
                    "Comparing times: local=%s, remote=%s",
                    datetime.fromtimestamp(local_mtime),
                    remote_dt,
                )

                # If remote is newer, file has been modified
                is_modified = remote_mtime > local_mtime
                return is_modified, remote_mtime

        except error.HTTPError as e:
            self.logger.debug("HEAD request failed: %s, assuming file modified", e)
            return True, None

    def _perform_download(
        self, remote_file: str, local_path: Path, reason: str
    ) -> bool:
        """
        Perform the actual file download using atomic write pattern.
        Downloads to temporary file first, then replaces target.

        For DBFS mounts (Azure), uses delete-then-rename pattern since
        os.replace() doesn't work reliably on FUSE-mounted cloud storage.

        Returns True if successful.
        Raises exceptions for caller to handle (404, EOFError, etc.)
        """
        self.logger.debug("Downloading %s. Reason: %s", local_path.name, reason)

        with request.urlopen(remote_file) as response:
            # Create temp file in same directory as target for atomic move
            temp_fd, temp_path = tempfile.mkstemp(
                dir=local_path.parent, prefix=f".{local_path.name}.", suffix=".tmp"
            )

            try:
                # Download to temp file
                with os.fdopen(temp_fd, "wb") as temp_file:
                    data = response.read()
                    temp_file.write(data)
                    temp_file.flush()  # Ensure data written to disk

                # Check if we're on DBFS mount (Azure/Databricks)
                is_dbfs = "dbfs" in local_path.parts

                if is_dbfs:
                    # DBFS mounts don't support atomic os.replace()
                    # Use delete-then-rename pattern for cloud storage
                    if local_path.exists():
                        os.remove(local_path)
                    os.rename(temp_path, local_path)
                    self.logger.debug(
                        "Successfully downloaded %s (DBFS delete-rename)",
                        local_path.name,
                    )
                else:
                    # Standard atomic replace for local filesystems
                    os.replace(temp_path, local_path)
                    self.logger.debug("Successfully downloaded %s", local_path.name)

                return True

            except Exception:  # pylint: disable=broad-except
                # Clean up temp file on error
                if os.path.exists(temp_path):
                    os.unlink(temp_path)
                raise

    def _download_http_file(self, remote_file: str, local_path: Path):
        """
        Download an http file with intelligent caching and atomic writes.

        Behavior depends on DownloadMode:
        - FORCE: Always download, regardless of local file state
        - NO_UPDATE: Skip if file exists locally
        - UPDATE: Download only if file doesn't exist or remote is newer

        Raises HTTPError, EOFError, etc. for caller to handle.
        """
        # Ensure parent directory exists
        local_path.parent.mkdir(parents=True, exist_ok=True)

        # Check if we should skip download
        should_skip, reason = self._should_skip_download(local_path, remote_file)
        if should_skip:
            self.logger.debug("Skipping %s. %s", local_path.name, reason)
            return

        # For UPDATE mode with existing file, check if remote is modified
        if (
            local_path.exists()
            and self._download_mode == DownloadMode.UPDATE
            and reason == "UPDATE mode - need to check remote"
        ):
            is_modified, _ = self._check_remote_modified(remote_file, local_path)
            if not is_modified:
                self._update_check_cache(remote_file)
                self.logger.debug(
                    "Skipping %s. Remote file not modified", local_path.name
                )
                return

            reason = "File has been modified"

        # Perform the download (may raise exceptions)
        success = self._perform_download(remote_file, local_path, reason)

        # Update cache after successful operation
        if success:
            self._update_check_cache(remote_file)

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

        self.logger.debug("Remote file: %s", remote_file)

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
