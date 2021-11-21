""" Virtual File System
    File system abstraction with implementations for HDFS and local files
"""
from abc import ABC, abstractmethod
import io
import json
import logging
import os
import pickle
import re
import tempfile
from typing import Any, Generator, Dict, List, Tuple
import urllib
from functools import total_ordering

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.csv as pc
import pyarrow.orc as po
import pyarrow.parquet as pq
import yaml

# try:
#     import pyorc
# except ModuleNotFoundError:
#     pyorc = None

from lvfs.stat import Stat


@total_ordering
class URL(ABC):
    #
    # Create URLs
    #

    """ Parsing and transformation of string-encoded URLs """

    def __init__(self, raw: str):
        """ Create a new URL from a string.

            Prefer URL.to unless you know what you're doing

            Accepts
            -------
            raw : str
                The raw string representation of the URL, to be parsed
        """
        self.raw: str = raw

    def __hash__(self):
        """ Support __hash__ based on the raw

            Returns
            -------
            int
                A uniformly-distributed but deterministic integral coimage corresponding to this
                URL's raw string representation
        """
        # Needed to sensibly use it as a dict key
        return hash(self.raw)

    def __eq__(self, value):
        """ Support __eq__ based on the raw string

            Accepts
            -------
            value : URL or str
                The URL to compare self to
        """
        # __ne__ delegates to __eq__ so we don't need __ne__
        return self.raw == value

    def __lt__(self, value):
        """ Comparison operators work on the raw string

            Accepts
            -------
            value : URL or str
                The URL to compare self to
        """
        # "@total_ordering class URL" handles the other operators
        return self.raw < value

    def __repr__(self) -> str:
        """ Show a URL in a user-readable format """
        return f"URL.to({repr(self.raw)})"

    @staticmethod
    def to(path):
        """ Placeholder for the actual implementation in lvfs.__init__
            This implemention exists only to satisfy type checking.

            Accepts
            -------
            path : URL or str
                URL passed through, or string to be parsed into a URL

            Returns
            -------
            URL
                A new object of a subclass of URL, dependent on protocol
        """
        # Totally bogus, merely to use the parameter and satisfy flake8
        return URL(path)

    #
    # Parse and modify URLs
    #
    def parse(self) -> urllib.parse.ParseResult:
        """ Parse the raw URL into parts using urllib
            Returns a ParseResult.

            Example:
            >>> urllib.parse.urlparse("derk://admin@uhhuh:8080/local/dir;xyz?key=val&key2=val2#4")
            ParseResult(
                scheme='derk', netloc='admin@uhhuh:8080', path='/local/dir', params='xyz',
                query='key=val&key2=val2', fragment='4'
            )
        """
        return urllib.parse.urlparse(self.raw)

    @property
    def protocol(self) -> str:
        """ Get the protocol of this url, or "file" if none """
        return self.parse().scheme or "file"

    @property
    def dirname(self) -> str:
        """ Get the path part of the parent of this URL, if it exists """
        return os.path.dirname(self.parse().path)

    @property
    def basename(self) -> str:
        """ Get the terminus of the path """
        return os.path.basename(self.parse().path)

    @property
    def host(self) -> str:
        """ Return the hostname, or bucket name in the case of GCS """
        return self.parse().netloc.split("@", 1)[-1]

    @property
    def user(self) -> str:
        """ Return the username to use for authentication. Useful for HDFS. """
        fields = self.parse().netloc.split("@", 1)
        return fields[0] if len(fields) == 2 else None

    def with_user(self, user: str):
        """ Change the username used for authentication. Useful for HDFS.

            Accepts
            -------
            user : str
                The username to associate with this URL

            Returns
            -------
            URL
                A new URL with the specified username and everything else the same
        """
        p = self.parse()
        fields = p.netloc.split("@", 1)
        p = p._replace(netloc=f"{user}@{fields[-1]}" if user else fields[-1])
        return URL.to(p.geturl())

    @property
    def path(self) -> str:
        """ Get just the path of this URL """
        return self.parse().path

    @property
    def parent(self):
        """ Get the parent of this URL, as a URL (as opposed to dirname, which is a str)

            Returns
            -------
            URL
                A new URL made from the parsed components.
        """
        # Accessing a private method -- but it seems pretty straightforward...
        parts = self.parse()
        return URL.to(parts._replace(path=os.path.dirname(parts.path)).geturl())

    def join(self, suffix):
        """ Return a new URL with another segment appended to the path

            Accepts
            -------
            suffix : str
                The segment to append to this URL.
                This does not accept URLs, only strings.

            Returns
            -------
            URL
                A new URL with the appended segment
        """
        if isinstance(suffix, URL):
            raise TypeError(
                "Can't join URLs. URLs are always absolute. Join a URL and a string instead."
            )
        return URL.to(self.raw.rstrip("/") + "/" + suffix)

    #
    # Abstract methods, *to be implemented by subclasses*
    #
    @abstractmethod
    async def read_binary(self) -> bytes:
        """ Read a file to a string of bytes """
        raise NotImplementedError

    @abstractmethod
    async def write_binary(self, content: bytes, overwrite: bool = True):
        """ Create or replace a file with a string of bytes

            Accepts
            -------
            content : bytes
                What to fill the target file with
            overwrite : bool
                Allow overwriting an existing file.
                Keep in mind that not every file system supports this concept,
                so with the specific implementations you plan to use.
        """
        raise NotImplementedError

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
        return True

    @abstractmethod
    async def ls(self, recursive: bool = False):
        """ Get the list of files in this directory, if it is one

            Returns a list of URL objects. Results are always absolute.
            *DO NOT `root.join(file)`*

            Returns
            -------
            List[URL]
                All the direct children, or all the recursive children
        """
        raise NotImplementedError

    @abstractmethod
    async def stat(self) -> Stat:
        """ Get basic stat results for this file """
        raise NotImplementedError

    @abstractmethod
    async def mkdir(self, ignore_if_exists: bool = False):
        """ Create an empty directory and parent directories recursively

            Accepts
            -------
            ignore_if_exists: boolean: DEPRECATED
                Included for backward compatibility. Existing directories are always ignored.
        """
        raise NotImplementedError

    def supports_permissions(self) -> bool:
        """ Some implementations, like blobs, do not always support permissions,
            If this method returns true, the file system supports permissions
        """
        return True

    async def chmod(self, mode: int):
        """ Modify permissions of the file so it has the desired mode """
        raise NotImplementedError

    @abstractmethod
    async def unlink(self, ignore_if_missing: bool = False):
        """ A lower-level method for removing one file or directory, to be overridden by specific
            URL implementations. The recursive rm may or may not be built on this.
        """
        raise NotImplementedError

    def supports_properties(self) -> bool:
        """ Return whether this URL supports setting key-value properties.
            Most filesystems do not, but this allows you to handle it programmatically,
            in most cases without any IO.
        """
        return False

    async def properties(self) -> Dict[str, List[str]]:
        """ Return the key-value properties associated with this URL.
            This is mostly for version control style filesystems.
            Most filesystems do not support this.
        """
        return NotImplementedError

    async def add_properties(self, **properties):
        """ Set a key-value property associated with this URL
            This is mostly for version control style filesystems.
            Most filesystems do not support this.
        """
        raise NotImplementedError

    async def delete_properties(self, names):
        """ Delete key-value properties from a URL.
            This is mostly for version control style filesystems.
            Most filesystems do not support this.
        """
        raise NotImplementedError

    #
    # Default serde implementations
    #
    async def read_pickle(self) -> Any:
        """ Read a pickle from a file """
        return pickle.loads(await self.read_binary())

    async def write_pickle(self, obj: Any):
        """ Write a pickle to a file """
        await self.write_binary(pickle.dumps(obj))

    async def read_yaml(self, **load_args) -> Any:
        """ Read a YAML from a file """
        return yaml.safe_load(await self.read_binary(), **load_args)

    async def write_yaml(self, obj: Any, **dump_args):
        """ Write a YAML to a file """
        await self.write_binary(yaml.dump(obj, **dump_args).encode())

    async def read_json(self, **load_args) -> Any:
        """ Read a JSON from a file """
        return json.loads(await self.read_binary(), **load_args)

    async def write_json(self, obj: Any, **dump_args):
        """ Write a JSON to a file """
        await self.write_binary(json.dumps(obj, **dump_args).encode())

    async def read_text(self) -> str:
        """ Decode the binary data as UTF8 """
        return (await self.read_binary()).decode()

    async def write_text(self, text: str):
        """ Encode the binary data as UTF8 """
        await self.write_binary(text.encode())

    async def _read_file(self, parser, *, recursive: bool = False):
        """ Read a file using the given parser. """
        shards = []
        partition_parser = re.compile(r"([^/=]+=[^/=]+)")
        if await self.isdir():
            targets = sorted(await self.ls(recursive=recursive))
        else:
            targets = [self]
        for child_url in targets:
            try:
                child_bytes = await child_url.read_binary()
            except IsADirectoryError:
                # That's fine, move on
                continue
            if child_bytes:
                # Some files are just empty sentinels
                shards.append(
                    parser(io.BytesIO(child_bytes)).assign(
                        **dict(
                            s.split("=")
                            for s in partition_parser.findall(child_url.dirname)
                        )
                    )
                )
        return pd.concat(shards)

    @staticmethod
    def _as_numpy_column(num_rows, pa_schema, pa_col, decimal_as="float"):
        """ Convert a pyarrow column to a numpy column
            lossily downcasting Decimals to float64 or int64

            Parameters
            ----------
            pa_schema: a Pyarrow schema, as taken from a Pyarrow RecordBatch
            pa_col: the corresponding Pyarrow column, taken from the same RecordBatch
            decimal_as: one of "float", "int":
                if "float": apply the decimal scale to get the closest possible float
                if "int": return an int64 array in multiples of the scale.
                For example: for scale=4, the float 5.1234 is 51234 as int.

            Returns
            -------
            a numpy array with the resulting data

            Notes
            -----
            * Decimal arrays can be NULL, but NULLs will be replaced with 0
            for integers, and with NaN for floats.
        """
        if isinstance(pa_schema.type, pa.Decimal128Type):
            # High precisions are not supported
            # Pyarrow ORC files are stored as two streams, a bit-packed PRESENT stream,
            # and a decimal128 stream of only the elements where PRESENT=True
            # We will read the buffer on 128-bit signed ints directly, and since numpy
            # only supports 64 bit ints we will truncate them to that.

            # Somehow the pa_col.buffers() could contain None value.
            valid_buffer = ([x for x in pa_col.buffers() if x] or [None])[0]
            present = np.frombuffer(valid_buffer, dtype=np.uint8)
            present = np.unpackbits(present, count=num_rows).astype(bool)
            present_ints = np.frombuffer(
                pa_col.buffers()[1], dtype=np.int64
            )[::2][:np.count_nonzero(present)]

            if decimal_as == "int":
                ints = np.zeros(num_rows, dtype=np.int64)
                ints[present] = present_ints
                return np.ma.masked_array(ints, mask=~present)
            elif decimal_as == "float":
                floats = np.full(num_rows, np.nan)
                floats[present] = present_ints * 10 ** -pa_schema.type.scale
                return np.ma.masked_array(floats, mask=~present)
            elif decimal_as == "decimal":
                raise NotImplementedError(
                    "Decimal passthrough is not supported in this version of LVFS"
                )
            else:
                raise NotImplementedError(
                    "Decimals must be returned as either float or int"
                )
        elif pa_schema.type == "date32[day]":
            # PyArrow has a bug where it reads 32 bit date types as 64 bit date types.
            # As a result, reading to a numpy array will fail because it isn't a multiple of the
            # element size. And when using pandas, it will have either half as many elements, with
            # the wrong values, or one less than half. In order to work around this error, we need
            # to request the buffers and the reread them with the correct format.
            valid_buffer = ([x for x in pa_col.buffers() if x] or [None])[0]
            present = np.frombuffer(valid_buffer, dtype=np.uint8)
            present = np.unpackbits(present, count=num_rows).astype(bool)
            present_ints = np.frombuffer(
                pa_col.buffers()[1], dtype=np.int32
            ).astype('datetime64[D]')[:np.count_nonzero(present)]

            dates = np.zeros(num_rows, dtype='datetime64[D]')
            dates[present] = present_ints[:num_rows]
            return np.ma.masked_array(dates, mask=~present)
        else:
            #try:
            #TODO: Check whether the problem has been fixed.
            return pa_col.to_numpy()
            # pyarrow.Array.to_numpy() doesn't support non-primitive types
            # until v0.17 (zero_copy_only=False), so use to_pandas() as a temp
            # workaround now, but need to check the content and consistency?
            # If we don't need the orc support on pyarrow, maybe we don't have
            # to stick with v0.13 anymore?
            # except NotImplementedError:
            #     # to_pandas() will sometimes return numpy arrays already, which dont have to_numpy()
            #     pandas_obj = pa_col.to_pandas()
            #     if hasattr(pandas_obj, "to_numpy"):
            #         return pandas_obj.to_numpy()
            #     else:
            #         return pandas_obj

    async def read_csv(self, *, recursive: bool = False, **pandas_args) -> pd.DataFrame:
        """ Read one or many csv files

            Accepts
            -------
            recursive: bool: Whether to read all CSV files within the directory
            pandas_args: dict: any other arguments to pass to read_csv(), which is very flexible

            Notes
            -----
            - The CSV serialization library may one day change (on a minor version bump), in which
              case extensively customizing the serialization may incur tech debt
            - Recursion is not supported when writing, so round-trips require you to write to
              a specific file, not a whole directory!
        """
        return await self._read_file(lambda f: pd.read_csv(f, **pandas_args), recursive=recursive)

    async def write_csv(self, frame: pd.DataFrame, **pandas_args):
        """ Write exactly one CSV file (not a directory)

            Accepts
            -------
            frame: Pandas Dataframe: the frame to write to a file
            pandas_args: dict: any other arguments to pass to to_csv(), which is very flexible

            Notes
            -----
            The CSV serialization library may one day change (on a minor version bump), in which
            case extensively customizing the serialization may incur tech debt
        """
        file_handle = io.BytesIO()
        frame.to_csv(file_handle, **pandas_args)
        await self.write_binary(file_handle.getbuffer())

    async def read_parquet(self, *, recursive: bool = False) -> pd.DataFrame:
        """ Read one or many parquet files
            - If this is a directory, read all the parquet files within it.
            - If recursive, read all parquets descended from it ad infinitum
        """
        return await self._read_file(pd.read_parquet, recursive=recursive)

    async def write_parquet(self, parq: pd.DataFrame, **_opts):
        """ Write the given Pandas dataframe to a parquet file """
        table = pa.Table.from_pandas(parq)
        bytefile = io.BytesIO()
        pq.write_table(table, bytefile)
        await self.write_binary(bytefile.getvalue(), overwrite=True)

    async def read_orc(self, *, recursive: bool = False) -> pd.DataFrame:
        """ Read one or many ORC files
            - If this is a directory, read all the parquet files within it.
            - If recursive, read all ORCs descended from it ad infinitum
        """
        return await self._read_file(pd.read_orc, recursive=recursive)

    async def write_orc(self, orc: pd.DataFrame, **_opts):
        """ Write the given Pandas dataframe to an ORC file """
        table = pa.Table.from_pandas(orc)
        stream = pa.BufferOutputStream()
        po.write_table(table, stream)
        await self.write_binary(stream.getvalue().to_pybytes(), overwrite=True)

    async def read_ascii_table(self, column_names: List[str]) -> pd.DataFrame:
        """ Read a file or folder as an ASCII formatted table or a collection of them """
        return await self._read_file(lambda f: pd.read_table(f, sep="\x01", names=column_names))

    async def force_local(self):
        """ Get a local URL to this file, copying (non-recursively) if necessary.

            Some libraries (like HDF5 and SQLite) only support local filenames, not bytestrings or
            file-like objects. This can help you support those systems.

            NOTE: It is your responsibility to delete the files after you are done!

            Returns
            -------
            URL
                A new URL pointing to a local file with the same data
        """
        fd, name = tempfile.mkstemp()
        name = URL.to(name)
        await self.cp(name, recursive=False)
        # In most cases I don't think we need to keep it open because another lib will open it
        os.close(fd)
        return name

    #
    # Protocol-agnostic high level IO methods
    #
    async def deep_mtime(self, recursive: bool = False) -> Tuple[float, float]:
        """Get aggregate modification time for single file or files in a dir.

        Returns:
            The (oldest, newest) modified time among all the files scanned
        """

        try:
            mtimes = [u.stat().mtime for u in await self.ls(recursive=recursive)]
            return (min(mtimes), max(mtimes))
        except Exception:  # TODO: which exception exactly is this trying to catch?  Be explicit.
            # False modification times are considered a recoverable error.
            logging.exception("Could not get modification times for %s", self.raw)
            return (0, 0)

    async def walk(self, topdown: bool = True):
        """ Get the list of files in this directory recursively, if it is one.
            Be careful, as this could be a gigantic list.
            It's read into memory but it's exposed as a generator.

            Returns
            -------
            a generator of (root, dirs, files) triples, where:
                root: a URL
                dirs: a list of URLs
                files: a list of URLs

            In all cases, all URLs are absolute. *DO NOT `root.join(file)`*
        """
        # NOTE: Some FS's have a better way to do this, consider overriding it.
        # TODO: Consider API exposing last modified time to prevent N+1 queries
        # but it's not clear how the API should look, because local filesystems
        # always require N+1 queries anyway
        kids = await self.ls(recursive=True)

        # Aggregate them by parent
        directories = {}
        for kid in kids:
            parent = kid.dirname()
            dirs, files = directories[parent] = directories.get(parent, ([], []))
            if await kid.isdir():
                dirs.append(kid)
            else:
                files.append(kid)

        # Sort for topdown-or bottom up
        for parent in sorted(directories.keys(), reverse=not topdown):
            dirs, files = directories[parent]
            yield (parent, dirs, files)

    async def cp(self, destination, recursive: bool = True):
        """ Copy a file from any system to any other. Buffers content in memory.

            Keep some caveats in mind:
                * This won't work for files too large for memory.
                * Some filesystems (GCS) don't directly support empty directories
                * This doesn't copy most permissions and metadata

            If recursive=True, it does not create an extra subdirectory, so:
                    `URL.to("/a/b").cp("/d/e", recursive)`
                    will copy `/a/b/x/y` to `/d/e/x/y`, not to `/d/e/b/x/y`
        """
        destination = URL.to(destination)
        if recursive and await self.isdir():
            await destination.mkdir()
            for name in await self.ls():
                await name.cp(destination.join(name.basename), recursive=True)
        else:
            await destination.write_binary(await self.read_binary())

    async def rm(self, recursive: bool = True, ignore_if_missing: bool = False):
        """ Remove self, and potentially everything under me. """
        if recursive and await self.isdir():
            for name in await self.ls():
                await name.rm(recursive=True, ignore_if_missing=ignore_if_missing)
        # In both cases, delete self afterward
        await self.unlink(ignore_if_missing=ignore_if_missing)

    async def mv(self, destination):
        """ Move self, including children in the case of a directory.
            Unlike rm and cp, this is always recursive.
        """
        await self.cp(destination, recursive=True)
        await self.rm(recursive=True)

    async def isdir(self) -> bool:
        """ Test whether this is a directory.

            This has caveats for file systems without directories, like blob stores.
            Empty directories in particular may not actually exist in any meaningful sense.
        """
        try:
            stat = await self.stat()
            return stat.kind == "directory"
        except IOError:
            # There are no unknowns in the normal case
            return False

    async def exists(self) -> bool:
        """ Test whether a file exists, coercing errors to False. """
        try:
            return await self.stat() is not None
        except IOError:
            return False

    async def du(self) -> int:
        """ Get the total size of a file or directory and its content """
        try:
            return sum(await kid.du()
                       for kid in await self.ls()
                       if kid != self
                       ) + (await self.stat()).size
        except NotADirectoryError:
            return (await self.stat()).size

    async def read_stream(self):
        """ Yield bytes from a file in whatever blocks are convenient for the filesystem.

            Notes
            -----
            The implementation is free to read the whole file if necessary -
            some systems cannot work any other way.
        """
        yield await self.read_binary()

    async def write_stream(self, gen):
        """ Fill a file from a generator of bytes objects

            Notes
            -----
            The implementation is free to write the whole file if necessary -
            some systems cannot work any other way.
        """
        chunks = [chunk async for chunk in gen]
        await self.write_binary(b"".join(chunks))

    async def make_bucket(self):
        """ Make a bucket.

            Notes
            -----
            The path is not used and no folders are created.
            For filesystems without buckets, this method has no effect.
            In other filesystems you may need special permissions to create buckets.
            Creating buckets programmatically may be unwise on account of billing.

            Errors
            ------
            Creating a bucket that already exists will fail with an error,
            provided the filesystem supports buckets and can recognize the bucket exists.
        """
        pass
