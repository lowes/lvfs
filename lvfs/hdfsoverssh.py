"""
WebHDFS based LVFS Adapter
==========================

This module is extremely flexible and supports many modes of operation but configuring it properly
can be challenging. Most of the time you will only need a few common configurations. Refer to the
project homepage for examples. Your company may also have recommended configurations, so check for
internal corporate documentation as well.

"""

from collections import namedtuple
import subprocess
import secrets
import time
import logging
import atexit
from typing import List, Optional
from threading import Thread

import hdfs
import requests
try:
    from requests_gssapi import HTTPSPNEGOAuth
except ModuleNotFoundError as err:
    HTTPSPNEGOAuth = err

from lvfs.url import URL
from lvfs.stat import Stat
from lvfs.credentials import Credentials

def _wrap_error(method):
    """ Wrap a function that throws HDFSError
        so that it throws the right IOErrors instead """
    async def inner(*args, **kwargs):
        try:
            return await method(*args, **kwargs)
        except hdfs.util.HdfsError as err:
            if "not found" in err.message:
                raise FileNotFoundError from err
            elif "not a directory" in err.message:
                raise NotADirectoryError from err
            elif "is a directory" in err.message:
                raise IsADirectoryError from err
            raise IOError from err
    return inner


HoSCreds = namedtuple("HoSCreds", [
    "username",
    "password",
    "ssh_username",
    "ssh_jump_host",
    "webhdfs_root"
])

class HDFSOverSSH(URL):
    """ Connect to HDFS via the CLI on the other end of an SSH tunnel """
    __tunnel: Optional[subprocess.Popen] = None
    __tunnel_port: Optional[int] = None
    __client: Optional[hdfs.InsecureClient] = None
    __canary: Optional[Thread] = None

    @classmethod
    def __creds(cls):
        # For the time being, we only support protocol-level credential realms
        creds = Credentials.match(classname="HDFSOverSSH")

        ssh_jump_host = creds.get("ssh_jump_host")
        ssh_username = creds.get("ssh_username")
        username = creds.get("username")
        password = creds.get("password")
        webhdfs_root = creds.get("webhdfs_root")

        #
        # See module level docstring for details on what this is doing.
        #
        if (ssh_jump_host is None) != (ssh_username is None):
            raise ValueError(
                "HDFSOverSSH needs either both ssh_jump_host and ssh_username or neither."
                f" For more information, see below:\n\n{__doc__}"
            )
        if username == "kerberos" and password is not None:
            raise ValueError(
                "HDFSOverSSH with kerberos doesn't support using passwords, which are only for AD"
                f" For more information, see below:\n\n{__doc__}"
            )
        if not webhdfs_root:
            raise ValueError(
                "HDFSOverSSH is missing webhdfs_root."
                " You probably need to update your lvfs.yml for new changes with HDP3."
                f" For more information, see below:\n\n{__doc__}"
            )
        return HoSCreds(username, password, ssh_username, ssh_jump_host, webhdfs_root)

    @classmethod
    def __setup_ssh_tunnel(cls):
        """ Setup an SSH SOCKS5 proxy.

            An proxy is not always required, so don't assume self.__tunnel exists.

            Returns
            -------
            int
                The port number associated with the proxy
        """
        #
        # Check if the tunnel is already setup and skip if it is.
        #
        if cls.__tunnel is None or cls.__tunnel.poll() is not None:
            if cls.__tunnel is not None:
                # The tunnel has died. Collect the zombie process.
                logging.info("HDFS SSH tunnel disconnected. Reconnecting.")
                cls.__tunnel.wait()

            # Start SSH to get the tunnel going
            cls.__tunnel_port = 30000 + secrets.randbelow(20000)
            cls.__tunnel = subprocess.Popen([
                "ssh", "-N", "-D", str(cls.__tunnel_port),
                f"{cls.__creds().ssh_username}@{cls.__creds().ssh_jump_host}"
            ])

            def deltunnel():
                """ Promptly disconnect the SSH tunnel when exiting Python,
                    to avoid resource leaks
                """
                cls.__tunnel.terminate()
                cls.__tunnel.wait()
            atexit.register(deltunnel)  # This will run this function when python exits

            def canary():
                """ Occasionally send small requests to keep the SSH tunnel open and avoid it
                    getting disconnected due to inactivity.
                    No special work is necessary to rebuild the connection. If the connection
                    was lost, it will be reconnected automatically as a result of this request.
                    This can also help diagnose problems quickly if network issues arise with any
                    long running process that may be idle a while (like APIs or event listeners)
                """
                while time.sleep(25):
                    try:
                        URL.to("hdfs://").stat()
                    except IOError as ioe:
                        raise RuntimeError("HDFS keepalive canary died") from ioe

            if cls.__canary is None or not cls.__canary.is_alive():
                cls.__canary = Thread(daemon=True, target=canary)
                cls.__canary.start()
        return cls.__tunnel_port

    @classmethod
    def __connect(cls):
        """ Connect to HDFS, configured as required in lvfs.yml.
            This may or may not start an SSH tunnel.

            Returns
            -------
            hdfs.Client or a subclass of it
                A client connected to HDFS.
                Despite the name InsecureClient, it may actually be secure if configured properly.
        """

        #
        # Check the tunnel is running if necessary
        #
        creds = cls.__creds()
        session = requests.Session()
        if creds.ssh_username:
            # This connection requires SSH tunneling
            port = cls.__setup_ssh_tunnel()
            session.proxies = {
                "http": f"socks5://localhost:{port}",
                "https": f"socks5://localhost:{port}"
            }

        # If the tunnel is still fine and the client is already built, then
        # just return the client immediately, probably everything is fine.
        # Otherwise move on to setting up the client.
        if cls.__client is not None:
            return cls.__client

        #
        # Setup Authentication
        #
        if creds.username == "kerberos":
            # This connection uses Kerberos authentication
            if isinstance(HTTPSPNEGOAuth, Exception):
                raise RuntimeError(
                    "requests-gssapi is not installed so Kerberos is not enabled."
                    " Install it, or install lvfs[all] to support all optional features."
                ) from HTTPSPNEGOAuth
            session.auth = HTTPSPNEGOAuth()
        elif creds.username is not None and creds.password is not None:
            # This connection uses AD authentication
            session.auth = requests.auth.HTTPBasicAuth(creds.username, creds.password)

        # Unfortunately it seems the certificates are self signed so we will have to ignore that
        session.verify = False

        cls.__client = hdfs.InsecureClient(
            url=creds.webhdfs_root,
            # This is not actually authenticated, it's trusted, you just pick a user.
            # It's done through a "user" parameter.
            # It is not used if you are also using AD or Kerberos, and it could cause
            # problems if you do so we avoid using that if AD or Kerberos is enabled
            user=(
                creds.username
                if creds.username not in ("kerberos", None) and creds.password is None
                else None
            ),
            session=session
        )
        if cls.__tunnel is not None:
            # Allow three seconds before the first check, only if using SSH
            time.sleep(3)
        for trials in range(10):
            # Try connecting immediately so that we catch connection errors immediately
            # rather than way later when they could be more difficult to spot
            try:
                cls.__client.list("/")
                break
            except requests.exceptions.ConnectionError as err:
                if trials == 9:
                    # If this fails, at least 9 seconds have passed
                    # so the error is probably real.
                    raise err
            time.sleep(1)

        return cls.__client

    @_wrap_error
    async def read_binary(self) -> bytes:
        """ Read a file to a string of bytes """
        with self.__connect().read(self.path) as reader:
            return reader.read()

    @_wrap_error
    async def write_binary(self, content: bytes, overwrite: bool = True):
        """ Write a whole file from bytes, overwriting if necessary

            Accepts
            -------
            content : bytes
                The bytestring to write into this new file, or to replace the previous file content
            overwrite : bool
                Whether to overwrite the file, if it exists.
        """
        with self.__connect().write(self.path, overwrite=overwrite) as writer:
            writer.write(content)

    @_wrap_error
    async def ls(self, recursive: bool = False) -> List[URL]:
        """ Get the list of files in this directory, if it is one

            Returns a list of URL objects. Results are always absolute.
            *DO NOT `root.join(file)`*
        """
        conn = self.__connect()
        try:
            if recursive:
                return [self] + [
                    URL.to(f"hdfs://{self.host}{root}/{kid_path}")
                    async for root, dirs, files in conn.walk(self.path)
                    for kid_path in dirs + files
                ]
            else:
                # Not recursive
                return [self.join(kid_path) for kid_path in conn.list(self.path)]
        except hdfs.util.HdfsError as err:
            if "not a directory" in err.message:
                return [self]
            else:
                raise err

    @_wrap_error
    async def stat(self) -> Stat:
        """ Get basic stat results for this file """
        # The JSON looks like
        # {
        # 'accessTime': 1587745709481,
        # 'blockSize': 268435456,
        # 'childrenNum': 0,
        # 'fileId': 1917677923,
        # 'group': 'hdfs',
        # 'length': 147603,
        # 'modificationTime': 1587745709673,
        # 'owner': 's0998yh0',
        # 'pathSuffix': '',
        # 'permission': '640',
        # 'replication': 3,
        # 'storagePolicy': 0,
        # 'type': 'FILE'
        # }
        stat = self.__connect().status(self.path)
        return Stat(
            self,
            kind=stat["type"].lower(),
            size=stat["length"],
            mtime=stat["modificationTime"] / 1000,
            atime=stat["accessTime"] / 1000,
            unix_permissions=int(stat["permission"], 8)
        )

    @_wrap_error
    async def mkdir(self, ignore_if_exists: bool = False):
        """ Create an empty directory and parent directories recursively

            Accepts
            -------
            ignore_if_exists: boolean: DEPRECATED
                Included for backward compatibility. Existing directories are always ignored.
        """
        self.__connect().makedirs(self.path)

    def supports_permissions(self) -> bool:
        """ Some implementations, like blobs, do not always support permissions,
            If this method returns true, the file system supports permissions
        """
        return True

    @_wrap_error
    async def chmod(self, mode: int):
        """ Modify permissions of the file so it has the desired mode """
        self.__connect().set_permission(self.path, oct(mode)[2:])

    @_wrap_error
    async def unlink(self, ignore_if_missing: bool = False):
        """ A lower-level method for removing one file or directory, to be overridden by specific
            URL implementations. The recursive rm may or may not be built on this.
        """
        self.__connect().delete(self.path)

    @_wrap_error
    async def rm(self, recursive: bool = True, ignore_if_missing: bool = False):
        """ Delete a file or directory from HDFS.
            Recursive deletes are faster than using unlink() repeatedly.
        """
        self.__connect().delete(self.path, recursive=recursive)

    async def read_stream(self):
        """ Yield bytes from a file in whatever blocks are convenient for the filesystem.

            Notes
            -----
            The implementation is free to read the whole file if necessary -
            some systems cannot work any other way.
        """
        with self.__connect().read(self.path) as reader:
            yield reader.read(1 << 20)

    async def write_stream(self, gen):
        """ Fill a file from a generator of bytes objects

            Notes
            -----
            The implementation is free to write the whole file if necessary -
            some systems cannot work any other way.
        """
        with self.__connect().write(self.path) as writer:
            async for chunk in gen:
                writer.write(chunk)
