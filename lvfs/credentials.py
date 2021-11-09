
from typing import Any, List, Optional
from collections import namedtuple
from pathlib import Path
import yaml
import logging
from lvfs.vault import Vault

Realm = namedtuple("Realm", ["classname", "host", "bucket", "path"])


class Credentials:
    """ A global namespace for credentials used in LVFS """
    __creds = ()

    @classmethod
    def init_register(cls, search_paths: List[Path] = None):
        """ Read credential configuration from a file, if possible.

            This is called automatically by match() the first time to initialize the store.
            If you call register() first, the default configuration will not be loaded,
            so you can override it.

            Accepts
            -------
            location: List of Paths to try to read the configuration from (must be local)

            If location is not specified, the following locations will be searched:
            * `./lvfs.yml`
            * `~/.config/lvfs.yml`
            * `/etc/creds/lvfs.yml`
            * `/etc/secret/lvfs.yml`

            Configuration file format
            -------------------------
            LVFS configuration is a YAML formatted file like so:

            ```
            credentials:
                - realm:
                    classname: HDFSCLIOverSSH
                    host: seattlehighmem001
                  ssh_username: ltorvalds
                - realm:
                    classname: Artifactory
                    host: artifactory.example.com
                  username: ltorvalds
                  password: chilloutthisisnotmypassword
            ```

            Each implementation will have different options, but all have realms,
            and realms can have classname, host, bucket, and path. Only classname is required.

            You should definitely store it in a Vault, and if you do,
            you probably don't have to configure anything because it's already centralized.
        """
        # We can't use URL here because that would create a cycle.
        search_paths = search_paths or [
            Path("./lvfs.yml"),
            Path("~/.config/lvfs.yml"),
            Path("/etc/creds/lvfs.yml"),
            Path("/etc/secret/lvfs.yml"),
            Vault
        ]
        for loc in search_paths:
            try:
                text = (
                    Vault.default()["lvfs.yml"]
                    if loc is Vault else
                    loc.expanduser().absolute().read_text()
                )
                conf = yaml.safe_load(text)
                creds = conf.get("credentials", [])
                assert isinstance(creds, list),\
                    f"LVFS credential malformed. Credentials must be a list but it's {creds}"
                for cred in creds:
                    assert "realm" in cred,\
                        f"LVFS credential malformed. Realm missing from credential: {cred}"
                    realm = cred["realm"]
                    assert "classname" in realm,\
                        "LVFS credential malformed. Every realm must have a classname. "\
                        f"Realm: {realm}"
                    cls.register(
                        content=cred,
                        classname=realm["classname"],
                        host=realm.get("host"),
                        bucket=realm.get("bucket"),
                        path=realm.get("path")
                    )
                return
            except IOError:
                logging.debug("Configuration not found at %s", loc.as_posix())
        logging.warn(
            "No LVFS credentials found. Check out `Credentials.init_register()` for details."
        )
        # Notice we replaced () with []
        # Both are iterable, satisfying pylint.
        cls.__creds = []

    @classmethod
    def register(cls,
                 content: Any,
                 classname: str,
                 host: Optional[str] = None,
                 bucket: Optional[str] = None,
                 path: Optional[str] = None):
        """ Associate some credentials with a realm.

            Accepts
            -------
            * classname: Name of the URL subclass involved. Case sensitive.
            * host: host this applies to. None matches all hosts. Case insensitive.
            * bucket: bucket this applies to. None matches all buckets. Case insensitive.
            * path: prefix of the path this applies to. None matches all paths. Case sensitive.
            * content: anything JSON-safe needed for authorization
        """
        # Make a copy, to satisfy Pylint that this is a list.
        creds = list(cls.__creds)
        host = None if host is None else host.lower()
        bucket = None if bucket is None else bucket.lower()
        creds.append((
            Realm(classname, host, bucket, path),
            content
        ))
        cls.__creds = creds

    @classmethod
    def match(cls,
              classname: str,
              host: Optional[str] = None,
              bucket: Optional[str] = None,
              path: Optional[str] = None,
              fail: bool = True):
        """ Search for credentials for a specific realm.

            This will return the first match.
            For best results, consider registering the most specific selectors first.

            Accepts
            -------
            * classname: Name of the URL subclass involved. Case sensitive.
            * host: host this applies to. None matches all hosts. Case insensitive.
            * bucket: bucket this applies to. None matches all buckets. Case insensitive.
            * path: prefix of the path this applies to. None matches all paths. Case sensitive.
            * content: anything JSON-safe needed for authorization
        """
        if cls.__creds == ():
            cls.init_register()
        host = None if host is None else host.lower()
        bucket = None if bucket is None else bucket.lower()
        query = Realm(classname, host, bucket, path)
        for realm, content in cls.__creds:
            if realm.classname != query.classname:
                continue
            if not (realm.host is None or realm.host == query.host):
                continue
            if not (realm.bucket is None or realm.bucket == query.bucket):
                continue
            if not (realm.path is None or query.path.startswith(realm.path)):
                continue
            return content

        if fail:
            raise ValueError(f"There is no realm matching {query}")
        else:
            return None  # Redundant but included for clarity
