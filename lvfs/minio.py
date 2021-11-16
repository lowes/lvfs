import io
from datetime import datetime
from typing import List, Dict, Any

import minio

from lvfs.url import URL
from lvfs.stat import Stat
from lvfs.credentials import Credentials

def _wrap_error(method):
    """ Wrap a function that throws MinioError
        so that it throws the right IOErrors instead """
    async def inner(*args, **kwargs):
        try:
            return await method(*args, **kwargs)
        except minio.error.NoSuchBucket as e:
            raise FileNotFoundError(e)
        except minio.error.NoSuchKey as e:
            raise FileNotFoundError(e)
        except minio.error.BucketAlreadyExists as e:
            raise FileExistsError(e)
        except minio.error.MinioError as e:
            raise IOError(e)
    return inner

class Minio(URL):
    """ Connect to HDFS via the CLI on the other end of an SSH tunnel """
    __clients: Dict[Any, minio.Minio] = {}

    @property
    def __creds(self):
        path = self.path
        bucket, path = path.split("/", 1) if "/" in path else (path, None)
        creds = Credentials.match(
            classname="Minio",
            host=self.host,
            bucket=bucket,
            path=path
        )

        assert "access_key" in creds, "Minio credentials missing access_key"
        assert "secret_key" in creds, "Minio credentials missing secret_key"
        # Return a tuple rather than a dict so it's hashable
        return (creds["access_key"], creds["secret_key"], bool(creds.get("secure")))

    def __bucket(self, required=True):
        """ Get the bucket from this URL.

            Accepts
            -------
            required: Whether to raise an error if there is no bucket specified
        """
        path = self.path
        path = path[1:] if path.startswith("/") else path
        bucket = path.split("/", 1)[0] if "/" in path else path
        if required and not bucket:
            raise ValueError(f"No bucket specified for Minio URL {self}")
        return bucket

    def __path_without_bucket(self):
        """ Get the path without the bucket from this URL.

            Accepts
            -------
            required: Whether to raise an error if there is no path specified
        """
        path = self.path
        path = path[1:] if path.startswith("/") else path
        return path.split("/", 1)[1] if "/" in path else ""

    def __connect(self):
        host, creds = self.host, self.__creds
        if (host, creds) not in self.__clients:
            self.__clients[(host, creds)] = minio.Minio(
                self.host,
                access_key=creds[0],
                secret_key=creds[1],
                secure=creds[2]
            )
        return self.__clients[(host, creds)]

    @_wrap_error
    async def read_binary(self) -> bytes:
        """ Read a file to a string of bytes """
        # TODO: Minio supports getting subslices, and it may be faster than the whole blob
        #       This could be pretty nice compared to the whole blob
        try:
            response = self.__connect().get_object(self.__bucket(), self.__path_without_bucket())
            return response.read()
        finally:
            response.close()
            response.release_conn()

    @_wrap_error
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
        self.__connect().put_object(
            self.__bucket(),
            self.__path_without_bucket(),
            io.BytesIO(content),
            length=len(content)
        )

    @_wrap_error
    async def ls(self, recursive: bool = False) -> List[URL]:
        """ Get the list of files in this directory, if it is one

            Returns a list of URL objects. Results are always absolute.
        """
        bucket = self.__bucket()
        if bucket:
            prefix = self.__path_without_bucket()
            if prefix and not prefix.endswith("/"):
                prefix = prefix + "/"
            return [
                # These paths are relative to the bucket, but join()
                # is relative to this prefix. So slice it off.
                self.join(obj.object_name[len(prefix):])
                for obj in self.__connect().list_objects_v2(
                    bucket,
                    prefix=prefix,
                    recursive=recursive
                )
                # A precaution before slicing
                if obj.object_name.startswith(prefix)
            ]
        else:
            # List buckets
            return [
                buck.name
                for buck in self.__connect().list_buckets()
            ]

    @_wrap_error
    async def stat(self) -> Stat:
        """ Get basic stat results for this file """
        stat = self.__connect().stat_object(self.__bucket(), self.__path_without_bucket())
        lm = stat.last_modified
        return Stat(
            self,
            kind="directory" if stat.is_dir else "file",
            size=stat.size,
            mtime=datetime(lm.tm_year, lm.tm_mon, lm.tm_mday, lm.tm_hour, lm.tm_min, lm.tm_sec)
        )

    @_wrap_error
    async def isdir(self) -> bool:
        """ Return whether this path is a directory.
            This concept doesn't exist in a blob store. This is always False.
        """
        return False

    @_wrap_error
    async def mkdir(self, ignore_if_exists: bool = False):
        """ Create an empty directory and parent directories recursively

            Accepts
            -------
            ignore_if_exists: boolean: DEPRECATED
                Included for backward compatibility. Existing directories are always ignored.
        """
        # This doesn't really exist, but it sorta does for buckets

    @_wrap_error
    async def make_bucket(self):
        """ Create a bucket.

            The path is not used and no folders are created.
            For filesystems without buckets, this method has no effect.
            In other filesystems you may need special permissions to create buckets.
            Creating buckets programmatically may be unwise on account of billing.
        """
        self.__connect().make_bucket(self.__bucket())

    def supports_permissions(self) -> bool:
        """ Some implementations, like blobs, do not always support permissions,
            If this method returns true, the file system supports permissions
        """
        return False

    def supports_directories(self) -> bool:
        """ Return whether the protocol supports first-class directories.

            Notes
            -----
            If the filesystems support directories, then:
                - mkdir() and isdir() have meaning
                - mkdir() followed by isdir() should be True
            Otherwise:
                - mkdir() has no effect and isdir() degrades to best-effort
                  which usually means it will only be True if the directory has content
        """
        return False

    @_wrap_error
    async def unlink(self, ignore_if_missing: bool = False):
        """ A lower-level method for removing one file or directory.

            Accepts
            -------
            ignore_if_missing: ignored: Minio cannot raise an error if something is missing
                                        because it disagrees on whether directories exist.
        """
        try:
            self.__connect().remove_object(self.__bucket(), self.__path_without_bucket())
        except minio.error.NoSuchKey:
            pass
