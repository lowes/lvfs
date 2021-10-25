from typing import Generator, List, Tuple
from lvfs.url import URL
import os
import pandas as pd
import aiofiles
from lvfs.stat import Stat


class Local(URL):
    def __init__(self, path: str):
        """ Create a new Local URL and convert it to an absolute path """
        super().__init__(os.path.abspath(path))

    """ VFS implementation specific to Local filesystem """

    async def read_binary(self) -> bytes:
        """ Read a whole file as bytes """
        with open(self.raw, "rb") as f:
            return f.read()

    async def write_binary(self, content: bytes, overwrite: bool = True):
        """  Write a whole file from bytes, overwriting if necessary

            Accepts
            -------
            content : bytes
                The bytestring to write into this new file, or to replace the previous file content
            overwrite : bool
                Whether to overwrite the file, if it exists. This is not atomic in this
                implementation.
        """
        if await self.exists() and not overwrite:
            raise FileExistsError(self)
        with open(self.raw, "wb") as f:
            return f.write(content)

    async def isdir(self):
        """ Is this path a directory (simpler because it's local) """
        return os.path.isdir(self.raw)

    async def ls(self, recursive: bool = False) -> List[URL]:
        """ Get the list of files in this directory, if it is one """
        if await self.isdir():
            return [
                self.join(xi)
                for xi in os.listdir(self.raw)
            ]
        else:
            return [self]

    async def walk(self,
                   topdown: bool = True
                   ) -> Generator[Tuple[URL, List[URL], List[URL]], None, None]:
        """ Get the list of files in this directory recursively, if it is one """
        for root, dirs, files in os.walk(self.raw, topdown=topdown):
            for kid in dirs + files:
                yield URL.to(root).join(kid)

    async def stat(self) -> Stat:
        """ Get basic stat results for this file """
        raw_stat = os.stat(self.raw)
        # Forgive me - this incurs like half a dozen syscalls but I don't see a better way ATM
        kinds = [
            (os.path.isfile(self.raw), "file"),
            (os.path.isdir(self.raw), "directory"),
            (os.path.islink(self.raw), "symlink"),
            (True, "other"),
        ]
        return Stat(
            url=self,
            kind=next(name for test, name in kinds if test),
            size=raw_stat.st_size,
            atime=raw_stat.st_atime,
            mtime=raw_stat.st_mtime,
            ctime=raw_stat.st_ctime,
            birthtime=raw_stat.st_ctime,
            unix_permissions=raw_stat.st_mode
        )

    async def mkdir(self, ignore_if_exists: bool = False):
        """ Create an empty directory and parent directories recursively

            Accepts
            -------
            ignore_if_exists: boolean: DEPRECATED
                Included for backward compatibility. Existing directories are always ignored.
        """
        os.makedirs(self.raw, exist_ok=True)

    async def chmod(self, mode: int):
        """ Modify permissions of the file so it has the desired mode """
        os.chmod(self.raw, mode)

    async def unlink(self, ignore_if_missing: bool = False):
        """ Remove a single file or directory """
        try:
            os.unlink(self.raw)
        except (IsADirectoryError, PermissionError):
            # Not sure why they call it permission error
            # but when you try to delete a directory using unlink it thorws this
            # If this fails just let the error propagate
            # Now it also raises IsADirectoryError.
            try:
                os.rmdir(self.raw)
            except OSError:  # In case the dir is not empty.
                import shutil
                shutil.rmtree(self.raw)
        except FileNotFoundError as ex:
            if not ignore_if_missing:
                raise ex

    async def force_local(self) -> URL:
        """ Local files are already local. So this is a no-op. """
        return self

    async def read_stream(self):
        """ Yield bytes from a file in whatever blocks are convenient for the filesystem.

            Notes
            -----
            The implementation is free to read the whole file if necessary -
            some systems cannot work any other way.
        """
        async with aiofiles.open(self.raw, "rb") as fh:
            yield await fh.read(1 << 20)

    async def write_stream(self, gen):
        """ Fill a file from a generator of bytes objects

            Notes
            -----
            The implementation is free to write the whole file if necessary -
            some systems cannot work any other way.
        """
        async with aiofiles.open(self.raw, "wb") as fh:
            async for chunk in gen:
                await fh.write(chunk)
