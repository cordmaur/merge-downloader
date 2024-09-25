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
from enum import Enum

# from dateutil import parser


class ConnectionType(Enum):
    """Enum to specify connection type (ftp or http)"""

    FTP = "ftp"
    HTTP = "http"


class DownloadMode(Enum):
    """Enum to specify download mode:
    - FORCE: if the file already exists, it will be overwritten
    - UPDATE: if the file already exists, update it if necessary (default)
    - NO_UPDATE: if the file already exists, it will not be downloaded
    """

    FORCE = "overwrite"
    UPDATE = "append"
    NO_UPDATE = "no_update"


class FileDownloader:
    """FTP helper class to download file preserving timestamp and to get file info, among others"""

    LOGGER = logging.getLogger(__name__)

    def __init__(
        self,
        server: str,
        connection_type: ConnectionType = ConnectionType.HTTP,
        download_mode: DownloadMode = DownloadMode.UPDATE,
        log_level: int = logging.INFO,
    ):
        """
        Initialize a FileDownloader instance

        Args:
            url (str): URL to the server.
            connection_type (Union[str, ConnectionType], optional): Type of connection.
            Defaults to "http".

        """
        if connection_type == ConnectionType.FTP:
            raise NotImplementedError("FTP connection is not yet implemented")

        # save the url and type of connection
        self._server = server
        self._connection_type = connection_type
        self._download_mode = download_mode

        # if it is an FTP connection, the ftp and context variables will be set up
        if self._connection_type == ConnectionType.FTP:
            self._context = ssl._create_unverified_context()  # pylint: disable=W0212
            self._ftp = FileDownloader.open_ftp_connection(server)

        # print the representation of the object
        self.logger = FileDownloader._setup_logger(log_level)
        self.logger.info(self.__repr__())

    # -------------------- Logging Functionality --------------------
    @staticmethod
    def _setup_logger(log_level: int) -> None:
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

            except Exception as error:  # pylint: disable=broad-except
                msg = f"Attempt {attempt + 1} to connect failed. "
                msg += f"Exception {type(error)}: {error}"

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

        # Then, let's open a request to the file to check its date
        with request.urlopen(remote_file) as response:
            remote_dt_str = response.headers.get("Last-Modified")
            date_format = "%a, %d %b %Y %H:%M:%S %Z"
            remote_dt = datetime.strptime(remote_dt_str, date_format)
            remote_mtime = time.mktime(remote_dt.timetuple())

            # If it exists and mode is UPDATE, check if the file has been modified or not
            if local_path.exists() and self._download_mode == DownloadMode.UPDATE:
                local_mtime = local_path.stat().st_mtime

                # if dates are the same, just skip because file already updated
                if local_mtime == remote_mtime:
                    self.logger.debug(
                        "Skipping download of %s. File already updated", local_path.name
                    )
                    return
                else:
                    self.logger.debug(
                        "Downloading %s. File has been modified", local_path.name
                    )
            elif not local_path.exists():
                self.logger.debug(
                    "Downloading %s. File does not exist", local_path.name
                )
            elif self._download_mode == DownloadMode.FORCE:
                self.logger.debug(
                    "Downloading %s. File already exists and mode is FORCE",
                    local_path.name,
                )

            # Now, let's download the file
            with open(local_path, "wb") as out_file:
                data = response.read()
                out_file.write(data)

            # And update the modification time
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
                self.logger(e)

            except error.HTTPError as e:
                # if the error code is 404, we know that the file does not exists
                if e.code == 404:
                    self.logger.warn("File %r was not available.", filename)
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


__all__ = ["ConnectionType", "DownloadMode", "FileDownloader"]
