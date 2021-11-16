import socket
import subprocess
import datetime
from typing import List
import paramiko
import time
import logging
from collections import namedtuple
from lvfs.url import URL
from lvfs.stat import Stat
from lvfs.credentials import Credentials

HCoSCreds = namedtuple("HCoSCreds", ["user", "jump_host"])

class HDFSCLIOverSSH(URL):
    """ Band-aid solution to HDFS over SSH, using the HDFS CLI client """

    __conn: paramiko.SSHClient = None

    @classmethod
    def __creds(cls):
        # For the time being, we only support protocol-level credential realms
        creds = Credentials.match(classname="HDFSCLIOverSSH")
        assert "ssh_jump_host" in creds, \
            "You must provide an ssh_jump_host in lvfs.yml to use HDFSCLIOverSSH"
        ssh_jump_host = creds.get("ssh_jump_host")
        # This is hdpdib for backward compatibility
        assert "ssh_username" in creds, \
            "You must provide an ssh_username in lvfs.yml to use HDFSCLIOverSSH"
        username = creds.get("ssh_username")
        return HCoSCreds(username, ssh_jump_host)

    @classmethod
    def __connect(cls, safe=True):
        if cls.__conn is None:
            creds = cls.__creds()
            cls.__conn = paramiko.SSHClient()
            cls.__conn.connect(creds.jump_host, username=creds.user, compress=True)
        try:
            cls.__conn.get_transport().send_ignore()
            return cls.__conn
        except paramiko.ssh_exception.SSHException as e:
            if safe:
                logging.info("LVFS: HDFSCLIOverSSH connection lost. Retrying.")
                return cls.__connect(safe=False)
            else:
                raise e

    def _cli(
        self,
        args,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=False,
        content=None,
        base="hdfs dfs "
    ):
        conn = self.__connect()
        # We have to do a little more work to escape arguments for paramiko than subprocess
        # Namely, paramiko only supports string commands (not list of string)
        command = (arg.replace(r"'", r"'\''") for arg in args)  # Escape single quotes
        command = (f"'{arg}'" for arg in command)  # Wrap in single quotes
        command = base + " ".join(command)  # Format into a string

        with conn.get_transport().open_session() as channel:
            channel.exec_command(command)
            channel.setblocking(0)
            # The following lines about std* emulate `subprocess.Popen.communicate()`
            #
            # Four parts that may help deciphering this loop:
            # 1. If you do a blocking read on either stderr or stdout, and the buffer for the other
            #    is full, then you will deadlock.
            # 2. Just because recv_ready() is False doesn't mean some data isn't still on it's way
            # 3. exit_status_ready() and nothing else ready() may mean the data is still in flight
            # 4. An empty string means the stream has ended.
            # 5. Stdin has to be closed for some programs to terminate (e.g. `tee` or `cat`)
            # 6. send() is allowed to send any number of bytes but 0 so you need to check how much
            #    was sent
            idle = True
            in_cursor = 0
            out_buf = []
            err_buf = []
            while not (out_buf[-1:] == [b""] and err_buf[-1:] == [b""]):
                idle = True
                if content and in_cursor < len(content) and channel.send_ready():
                    in_cursor += channel.send(content[in_cursor: in_cursor + 1 << 16])
                    if in_cursor >= len(content):
                        channel.shutdown_write()
                    idle = False
                if not out_buf[-1:] == [b""]:
                    # If any stream returns "", that means it has reached EOF and will never
                    # return again
                    try:
                        out_buf.append(channel.recv(1 << 16))
                        idle = False
                    except socket.timeout:
                        pass
                if not err_buf[-1:] == [b""]:
                    try:
                        err_buf.append(channel.recv_stderr(1 << 16))
                        idle = False
                    except socket.timeout:
                        pass
                if idle and not channel.exit_status_ready():
                    time.sleep(0.05)
            exit_status = channel.recv_exit_status()
            stdout = b"".join(out_buf)
            stderr = b"".join(err_buf)

        if exit_status:
            # There was an error
            if b"File exists" in stderr:
                raise FileExistsError(self.raw)
            elif b"No such file or directory" in stderr:
                raise FileNotFoundError(self.raw)
            elif b"Is a directory" in stderr:
                raise IsADirectoryError(self.raw)
            else:
                raise IOError(stderr)
        if text:
            return stdout.decode(), stderr.decode()
        else:
            return stdout, stderr

    async def read_binary(self) -> bytes:
        """ Read a whole file as bytes """
        stdout, stderr = self._cli(["-cat", self.raw])
        return stdout

    async def write_binary(self, content: bytes, overwrite: bool = True):
        """ Write a whole file from bytes, overwriting if necessary

            Accepts
            -------
            content : bytes
                The bytestring to write into this new file, or to replace the previous file content
            overwrite : bool
                Whether to overwrite the file, if it exists.
                This implementation does not natively support that case, so it is emulated in a
                non-atomic way, which can produce bad results in frequently modified files.
                Since HDFS is already a terrible choice for such data we're letting that pass.
        """
        if await self.exists() and not overwrite:
            raise FileExistsError(self)
        self._cli(["-put", "-", self.raw], content=content)

    async def ls(self, recursive: bool = False) -> List[URL]:
        """ Get the list of files in this directory, if it is one """
        recursive = ["-R"] if recursive else []
        text = self._cli(["-ls"] + recursive + [self.raw], text=True)[0]
        return [
            URL.to(line.split(None, 7)[7])
            for line in text.splitlines()
            if not line.startswith("Found")
        ]

    async def stat(self) -> Stat:
        """ Get basic stat results for this file """

        """
        According to the Apache documentation:
        > Format accepts filesize in blocks (%b), type (%F), group name of owner (%g), name (%n),
        > block size (%o), replication (%r), user name of owner(%u),
        > and modification date (%y, %Y). %y shows UTC date as “yyyy-MM-dd HH:mm:ss”
        > and %Y shows milliseconds since January 1, 1970 UTC.
        > If the format is not specified, %y is used by default.

        But that doesn't seem to be accurate. Instead, %b gives the size in bytes,
        and %o gives something else (about 4x length in bytes?)

        Examples:
        ```
        lxhdpedgeqa001 [~]$ hdfs dfs -stat '%b|%o|%y|%F|%u|%g|%r' hdfs://yadayada
        94516491|268435456|2020-08-24 14:50:31|regular file|hdpbatch|hdfs|3
        lxhdpedgeqa001 [~]$ hdfs dfs -ls hdfs://yadayada
        -rwxr-xr-x   3 hdpbatch hdfs   94516491 2020-08-24 10:50 hdfs://yadayada
        ```
        """

        # In order to get size information and most details we need to use stat
        # But it doesn't give us permission information - only ls does
        stat = {}
        lines = self._cli(["-stat", "%b|%o|%y|%F|%u|%g|%r", self.raw])[0].splitlines()
        if lines:
            # Keep in mind this is UTC time so it might be confusing.
            # So we return it as a timezone-aware datetime
            (
                size_in_bytes,
                _what_is_this,
                mtime,
                kind,
                _username,
                _groupname,
                _replication,
            ) = lines[0].decode().split("|")
            stat = dict(
                url=self,
                kind="file" if kind == "regular file" else kind,
                size=int(size_in_bytes),
                mtime=datetime.datetime.strptime(
                    mtime + " +0000", "%Y-%m-%d %H:%M:%S %z"
                ),
            )
        else:
            raise FileNotFoundError(self.raw)

        # Continue with ls this time
        ls_target = self.parent.raw if stat["kind"] == "directory" else self.raw
        lines = [
            line
            for line in self._cli(["-ls", ls_target])[0].splitlines()
            if line.strip().endswith(b"/" + self.basename.encode())
        ]
        if lines:
            mode = lines[0].decode().split()
            mode = mode[0] if mode else b""
            # Convert -rwxrwxrwx format into octal
            mode = sum(
                1 << offset
                for offset, allowed in enumerate(mode[::-1])
                if allowed != "-")
            stat["unix_permissions"] = mode
        else:
            raise FileNotFoundError(self.raw)

        return Stat(**stat)

    async def mkdir(self, ignore_if_exists: bool = False):
        """ Create an empty directory and parent directories recursively

            Accepts
            -------
            ignore_if_exists: boolean: DEPRECATED
                Included for backward compatibility. Existing directories are always ignored.
        """
        self._cli(["-mkdir", "-p", self.raw])

    async def unlink(self, ignore_if_missing: bool = False):
        """ Remove a single file or directory """
        try:
            self._cli(["-rm", self.raw])
        except IsADirectoryError:
            # Fine, remove it as a directory
            self._cli(["-rmdir", self.raw])
        except FileNotFoundError as ex:
            if not ignore_if_missing:
                raise ex

    async def rm(self, recursive: bool = True, ignore_if_missing: bool = False):
        """ Remove a file or directory, maybe recursively """
        # Overrides the default method because it can be done in one call
        recursive = ["-r"] if recursive else []
        try:
            self._cli(["-rm"] + recursive + [self.raw])
        except FileNotFoundError as ex:
            if not ignore_if_missing:
                raise ex

    async def chmod(self, mode: int):
        """ Change the permissions of a file """
        self._cli(["-chmod", oct(mode)[2:], self.raw])
