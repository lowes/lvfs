from typing import Dict, List
from lvfs.stat import Stat
from lvfs.credentials import Credentials
from lvfs.url import URL
import requests
import datetime

class Artifactory(URL):
    """ Manage an artifactory repository using the REST API

        Artifactory URLs are formatted like so:
        `artifactory://hostname/bucket-name/path`
        so for example you might use:
        `URL.to("artifactory://artifactory.example.com/your-repo-name/your-directory/your-file.ext")`

        Configuration
        -------------
        This URL requires two fields to be specified in your configuration,
        like so:

        ```
        credentials:
            - realm:
                classname: Artifactory
                host: artifactory.example.com
              username: ltorvalds
              password: chilloutthisisnotmypassword
        ```

        Typically you would place this in your home directory as the file
        `~/.config/lvfs.yml`.
        If you have multiple realms, make sure not to duplicate the line
        `credentials:`.
    """
    # This API is somewhat like Webdav but it does have quirks.
    # For example, some URLs have /api/storage and some do not.

    @property
    def __creds(self):
        path = self.path
        bucket, path = path.split("/", 1) if "/" in path else (path, None)
        creds = Credentials.match(
            classname="Artifactory",
            host=self.host,
            bucket=bucket,
            path=path
        )
        assert "username" in creds, "Artifactory credentials missing username"
        assert "password" in creds, "Artifactory credentials missing password"
        return (creds["username"], creds["password"])

    @property
    def bucket(self):
        path = self.path
        return path.split("/", 1)[0] if "/" in path else path

    @property
    def path_without_bucket(self):
        path = self.path
        return path.split("/", 1)[1] if "/" in path else None

    def __request(self, method, url, **kwargs):
        """ Wrap requests so that errors are raised rather than ignored. """
        resp = requests.request(method=method, url=url, **kwargs)
        resp.raise_for_status()
        return resp

    async def read_binary(self) -> bytes:
        """ Read a file to a string of bytes """
        return self.__request(
            "GET",
            f"https://{self.host}/artifactory{self.path}",
            auth=self.__creds
        ).content

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
        self.__request(
            "PUT",
            f"https://{self.host}/artifactory{self.path}",
            data=content,
            auth=self.__creds
        )

    async def ls(self, recursive: bool = False):
        """ Get the list of files in this directory, if it is one

            Returns a list of URL objects. Results are always absolute.
            *DO NOT `root.join(file)`*
        """

        resp = self.__raw_stat()
        if "children" not in resp:
            raise NotADirectoryError(str(self))
        children = resp["children"]

        descendants = []
        for kid in children:
            kid_url = Artifactory(
                f"artifactory://{self.host}{self.path}{kid['uri']}"
            )
            descendants.append(kid_url)
            if recursive and kid["folder"]:
                descendants.extend(kid_url.ls(recursive=True))
        return descendants

    def __raw_stat(self):
        """ Get detailed information for a path """
        return self.__request(
            "GET",
            f"https://{self.host}/artifactory/api/storage{self.path}",
            auth=self.__creds
        ).json()

    async def stat(self):
        """ Get basic stat results for this file """
        resp = self.__raw_stat()
        assert "created" in resp, f"Artifactory stat missing 'created' field: {resp}"
        assert "lastModified" in resp, f"Artifactory stat missing 'lastModified' field: {resp}"
        return Stat(
            self,
            kind="file" if "size" in resp else "directory",
            size=int(resp.get("size", "0")),  # Directories are missing size
            ctime=datetime.datetime.fromisoformat(resp["created"]),
            # Also there is a user "createdBy" available
            mtime=datetime.datetime.fromisoformat(resp["lastModified"]),
            # Also there is a user "modifiedBy" available
            # There is also an updated date but I'm not sure what field that would map to
            unix_permissions=0o777  # Their permissions are not like unix
        )

    async def mkdir(self, ignore_if_exists: bool = False):
        """ Create an empty directory and parent directories recursively

            Accepts
            -------
            ignore_if_exists: boolean: DEPRECATED
                Included for backward compatibility.
                Existing directories are always ignored.
        """
        resp = requests.put(
            f"https://{self.host}/artifactory{self.path}{'' if self.path.endswith('/') else '/'}",
            auth=self.__creds
        )
        if not ignore_if_exists:
            resp.raise_for_status()

    def supports_permissions(self) -> bool:
        """ Artifactory does not support permissions in the Unix sense """
        return False

    async def chmod(self, mode: int):
        """ Modify permissions of the file so it has the desired mode """
        # This doesn't really apply to Artifactory.

    async def unlink(self, ignore_if_missing: bool = False):
        """ A lower-level method for removing one file or directory, to be overridden by specific
            URL implementations. The recursive rm may or may not be built on this.
        """
        # not using self.__request because we want to choose if failure is OK
        resp = requests.delete(
            f"https://{self.host}/artifactory{self.path}",
            auth=self.__creds
        )
        if not ignore_if_missing:
            resp.raise_for_status()

    @staticmethod
    def __clean(text):
        """ Escape certain characters so be safe as URL-encoded.
            The exact escape was specified by the Artifactory documentation
        """
        return (
            text.replace(",", "%5C,")
            .replace("\\", "%5C\\")
            .replace("|", "%5C|")
            .replace("=", "%5C=")
        )

    def supports_properties(self) -> bool:
        """ Return whether this URL supports setting key-value properties.
            Most filesystems do not, but this allows you to handle it programmatically,
            in most cases without any IO.
        """
        return True

    async def properties(self) -> Dict[str, List[str]]:
        """ Return the key-value properties associated with this URL.
            This is mostly for version control style filesystems.
            Most filesystems do not support this.
        """
        resp = self.__request(
            "GET",
            f"https://{self.host}/artifactory/api/storage{self.path}?properties",
            auth=self.__creds
        ).json()
        assert "properties" in resp, f"Artefactory missing properties in properties() call: {resp}"
        return resp["properties"]

    async def add_properties(self, **properties: List[str]):
        """ Set a key-value property associated with this URL
            This is mostly for version control style filesystems.
            Most filesystems do not support this.

            Accepts
            -------
            * `**properties`: Dict[str, List[str]] specified as kwargs
        """
        properties = ";".join(
            f"{self.__clean(k)}={self.__clean(v)}"
            for k, v in properties.items()
        )
        resp = self.__request(
            "PUT",
            f"https://{self.host}/artifactory/api/storage{self.path}?properties={properties}",
            auth=self.__creds
        ).json()
        assert "properties" in resp, f"Artefactory missing properties in properties() call: {resp}"
        return resp["properties"]

    async def delete_properties(self, properties: List[str]):
        """ Delete key-value properties from a URL.
            This is mostly for version control style filesystems.
            Most filesystems do not support this.
        """
        properties = ",".join(self.__clean(k) for k in properties)
        self.__request(
            "DELETE",
            f"https://{self.host}/artifactory/api/storage{self.path}?properties={properties}",
            auth=self.__creds
        )
