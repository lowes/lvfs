""" Convenient high level file IO across multiple protocols

Supported Protocols
===================
* Local files
* HDFS (the method is automatically chosen)
    * HDFS binary protocol
    * HDFS as HTTP over SSH for remote development
    * HDFS as CLI over SSH for emergency situations
* Google Cloud Storage (GCS)
* Artifactory, for model and code reading and writing
"""

__version__ = "1.1.2"
__all__ = ["URL"]
import copyreg
import urllib
from lvfs.url import URL
from lvfs.local import Local

def __not_installed(error):
    """ Wrap an ImportError in a function so it can fail when used instead of when imported

        Accepts
        -------
        error : ImportError
            The Error to complain about

        Returns
        -------
        a function that will throw the error in question when called
    """
    def fail():
        raise NotImplementedError(
            "LVFS adapter is disabled because its module could not be imported. "
            "Please make sure you have the necessary python packages installed, "
            "or if you're not sure, try installing lvfs[all], which will enable all the features "
            "but may be a a little heavy on the dependencies for a production deployment."
        ) from error
    return fail


#
# Import adapters so we can add them to the registry
# but don't use them yet, so they don't fail if we don't have them.
# Also, don't wrap them in a function, so that way users can mutate the registry if needed
#
try:
    from lvfs.hdfsoverssh import HDFSOverSSH
except ImportError as err:
    HDFSOverSSH = __not_installed(err)

try:
    from lvfs.gcs import GCS
except ImportError as err:
    GCS = __not_installed(err)

try:
    from lvfs.minio import Minio
except ImportError as err:
    Minio = __not_installed(err)

try:
    from lvfs.artifactory import Artifactory
except ImportError as err:
    Artifactory = __not_installed(err)

# The adapter associated with each protocol.
# This is global because it effects the URL.to method, which could be called anywhere.
# It is mutable because you may need to update it if you are in an unusual circumstance
# and you need a different adapter than most people might.
# For example, if you need to access Artifactory but you need to handle proxying yourself,
# maybe you would want to subclass Artifactory, and then delegate all artifactory://
# URLs to your new adapter.
protocol_registry = {
    "file": Local,
    "hdfs": HDFSOverSSH,
    "gs": GCS,
    "s3": Minio,
    "minio": Minio,
    "artifactory": Artifactory,
}

@staticmethod
def to(path):
    """ Create a new URL, or pass an existing URL through.

        It's namespaced in URL but defined in lvfs.__init__ because of import cycles.
        This is intended as a convenience method for an extremely common use case where
        you want to instantiate a new URL, and which subclass depends on the protocol.

        Accepts
        -------
        path : URL or str
            Another URL, or a raw string to be interpreted as a URL

        Returns
        -------
        URL
            If path was a URL: The same URL
            If path was a str:
                It will be parsed as a URL
                If its protocol is in lvfs.protocol_registry,
                    You'll get an instance of that class
                If not, or if that feature was not installed,
                    You'll get an error
    """
    if isinstance(path, URL):
        return path
    else:
        proto = urllib.parse.urlparse(path).scheme or "file"
        try:
            return protocol_registry[proto](path)
        except KeyError as keyerror:
            raise ValueError(f"No handler registered for {proto}") from keyerror


# It is not possible to define this static method in URL without creating a loop.
# The subclasses of URL must import URL in order to reference the base class.
# But URL would have to import the subclasses in order to define URL.to.
URL.to = to

""" Register each type with copyreg so that pickle will serialize them properly

    Accepts
    -------
    u: An object of any subclass of URL
"""


def _pickle_url(u):
    return (URL.to, (u.raw,))


copyreg.pickle(URL, _pickle_url)
copyreg.pickle(Local, _pickle_url)

try:
    from lvfs.hdfs import HDFS
    copyreg.pickle(HDFS, _pickle_url)
except ImportError:
    # HDFS not included.
    pass

try:
    from lvfs.gcs import GCS
    copyreg.pickle(GCS, _pickle_url)
except ImportError:
    # GCS not included.
    pass

try:
    from lvfs.hdfsoverssh import HDFSOverSSH
    copyreg.pickle(HDFSOverSSH, _pickle_url)
except ImportError:
    # HDFSOverSSH not included.
    pass

try:
    from lvfs.hdfsclioverssh import HDFSCLIOverSSH
    copyreg.pickle(HDFSCLIOverSSH, _pickle_url)
except ImportError:
    # HDFSCLIOverSSH not included.
    pass

try:
    from lvfs.artifactory import Artifactory
    copyreg.pickle(Artifactory, _pickle_url)
except ImportError:
    # Artifactory not included.
    pass

try:
    from lvfs.minio import Minio
    copyreg.pickle(Minio, _pickle_url)
except ImportError:
    # Minio not included.
    pass
