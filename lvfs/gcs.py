from typing import List, Union
import urllib
from google.cloud import storage as gcs
from google.api_core.exceptions import NotFound
from lvfs.url import URL
from lvfs.stat import Stat


class GCS(URL):
    """ Implementation of URL based on GCS """

    _client = None

    def __init__(self, raw: Union[URL, str]):
        """ Create a new URL based on GCS """
        super().__init__(raw)
        # GCS doesn't accept leading /'s
        self._gpath = self.path.lstrip("/")
        self._gpath_length = len(self._gpath)
        if self._client is None:
            GCS._client = gcs.Client()
        self._bucket = GCS._client.get_bucket(self.host)

    async def read_binary(self) -> bytes:
        """ Read a whole GCS file into a bytes object and return it """
        return self._bucket.get_blob(self._gpath).download_as_string()

    async def write_binary(self, content: bytes, overwrite: bool = True):
        """ Write a whole file from bytes, overwriting if necessary

            Accepts
            -------
            content : bytes
                The bytestring to write into this new file, or to replace the previous file content
            overwrite : bool
                Whether to overwrite the file, if it exists.
                GCS supports atomic overwrites but it works using version numbers not compatible
                with most other file systems, so instead we emulate it with non-atomic operations
                here, which can be a problem for frequently modified files.
        """
        if await self.exists() and not overwrite:
            raise FileExistsError(self)
        self._bucket.blob(self._gpath).upload_from_string(content)

    async def ls(self, recursive: bool = False) -> List[URL]:
        """
        List a "directory" within GCS

        This is a very ugly procedure that requires listing all the "subdirectories"
        even if recursive=False, because otherwise we will not find the immediate subdirectories
        because they are not objects (they are only prefixes of filenames).

        Definitely avoid running this too often.
        Unfortunately, GCS.isdir requires this function to detect a subdirectory, which means
        GCS.isdir will also be very slow and expensive. Keep that in mind.
        """
        # TODO: This needs to be tested against a real GCS bucket
        kids = set()
        if not recursive:
            for name in self._bucket.list_blobs(prefix=self._gpath.rstrip("/")):
                kid_path = urllib.parse.unquote(name.path.split("/o/")[1])
                slash_after_kid_path_idx = kid_path.find("/", self._gpath_length)
                if slash_after_kid_path_idx > -1:
                    kids.add(kid_path[: slash_after_kid_path_idx + 1])
                else:
                    kids.add(kid_path)
            return [URL.to(f"gs://{self.host}/{kid_path}") for kid_path in kids]
        else:
            return list(self._bucket.list_blobs(prefix=self._gpath.rstrip("/")))

    async def stat(self) -> Stat:
        """ Get basic stat results for this file """
        blob = self._bucket.get_blob(self._gpath)

        s = Stat(
            url=self,
            size=blob.size,
            kind="file",  # GCS doesn't really have directories
            mtime=blob.updated,
            birthtime=blob.time_created,
            # This is not really accurate but they only use ACL in GCS
            unix_permissions=0o660
        )

        return blob and s

    async def mkdir(self, ignore_if_exists: bool = False):
        """ Create an empty directory and parent directories recursively

            Accepts
            -------
            ignore_if_exists: boolean: DEPRECATED
                Included for backward compatibility. Existing directories are always ignored.
        """
        pass

    async def rm(self, recursive: bool = True, ignore_if_missing: bool = False):
        """ Remove a blob and possibly all the blobs starting with a specific prefix

            GCS doesn't exactly have directories so ignore_if_missing in this implementation just
            check if at least one thing was deleted that had that prefix.
        """
        # This one is overridden because GCS doesn't care if you delete the parent first
        # so we can do this in much fewer calls.
        something_was_deleted = False
        if recursive:
            # NOTE: You can't just pass recursive to ls because then False means to delete just
            # one level down but we want False to mean delete only self.
            async for grandkid in self.ls(recursive=True):
                try:
                    grandkid.delete()
                    something_was_deleted = True
                except FileNotFoundError:
                    # NOTE: ignoring the error here is different from ignoring it inside unlink
                    # this way something_was_deleted will not be set if unlink fails
                    pass
            # Since there aren't directories, self could be a blob and a prefix
            # It's debatable whether you want to also delete self in that case but I side with yes.
            # So either way, delete self.
        try:
            self.unlink()
            something_was_deleted = True
        except (FileNotFoundError, NotFound):
            # same note as for the recursive case
            pass
        if not something_was_deleted and not ignore_if_missing:
            raise FileNotFoundError(self.raw)

    async def unlink(self, ignore_if_missing: bool = False):
        """ Remove a single blob """
        blob = self._bucket.get_blob(self._gpath)
        if blob:
            blob.delete()
        elif self._gpath.endswith("/"):
            # directories don't exist and we'll have removed everything inside
            pass
        elif not ignore_if_missing:
            raise FileNotFoundError(self.raw)
