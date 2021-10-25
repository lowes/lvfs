import datetime


class Stat:
    """ VFS stat() result

        This differs from the builtin os.stat_result in that:
        * It doesn"t include less commonly used attributes like inode number
        * It guesses fallbacks, like if mtime is null use ctime, etc.
        * Timestamps are datetime objects so they can be timezone aware
        * Includes the kind if available (one of "directory", "file", "symlink", "device",
            "other", None)
    """
    __slots__ = "url", "kind", "size", "atime", "mtime", "ctime", "birthtime", "unix_permissions"

    def __init__(
        self,
        url,
        kind=None,
        size=None,
        atime=None,
        mtime=None,
        ctime=None,
        birthtime=None,
        unix_permissions=None
    ):
        self.url = url
        kind = kind and kind.lower()
        self.kind = kind if kind in ["directory", "file", "symlink", "device"] else None
        self.size = size
        [atime, mtime, ctime, birthtime] = [
            datetime.datetime.fromtimestamp(x) if type(x) in [float, int] else x
            for x in [atime, mtime, ctime, birthtime]
        ]
        self.atime = atime or mtime or ctime or birthtime
        self.mtime = mtime or ctime or birthtime or atime
        self.ctime = ctime or birthtime or mtime or atime
        self.birthtime = birthtime or ctime or mtime or atime
        self.unix_permissions = unix_permissions

    def __repr__(self):
        """ Represent a stat() in a more user-friendly text format """
        kvstr = ", ".join(f"{k}={repr(getattr(self, k))}" for k in self.__slots__)
        return f"Stat({kvstr})"
