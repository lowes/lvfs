L Virtual File System
=====================

Rationale
---------
LVFS is a Python interface wrapping several storage APIs, particularly HDFS, to give a generally consistent
abstraction over multiple APIs.

 * LVFS can allow you to shift existing or legacy Python applications from one storage API to another,
   and has been used to move production applications from HDFS to GCS, and from local files to HDFS.
 * LVFS doesn't require local temporary space; data doesn't need to save on disk when uploading or downloading
 * LVFS handles pretty complicated credential configurations, which can save a lot of time when developing
 * LVFS integrates several big data formats used in Hadoop, so analytics applications usually can begin using
   Hive or Spark data using only one line of code.
 * LVFS uses Python modules whenever possible and only requires `ssh` be installed if you need to use SSH.
   (This is because Paramiko does not support complex secured proxies.)

Comparison against other solutions
----------------------------------
LVFS is similar to some other storage tools:

### rclone
* `rclone` is a file sync utility that supports almost every imaginable storage API, with an interface very
  similar to `rsync`, except that `rsync` is older and only supports SSH and it's own custom protocol.
* Both systems create a common interface for multiple APIs
* LVFS separates reading from writing so you can read or write without touching local disk.
  This is important for using LVFS inside a cluster because this reduces disk activity by at least half.
  It is also important in containers, which often have very little available disk space (only memory)
* `rclone` supports way more protocols, where LVFS only supports the most common ones.
* `rclone` is a command line application, but LVFS is a Python library so integrating them into applications
  is much different.

### hdfs
There is a Python package named `hdfs` which supports WebHDFS accesses from Python, and it is the most closely
aligned open source project to LVFS. It is essentially an SDK for WebHDFS and supports everything `lvfs`
supports, because LVFS uses `hdfs`.

* LVFS supports more than one protocol, which is important if you are using LVFS to "lift and shift" a legacy
  application between storage APIs
* `hdfs` supports a more thorough and complete API around HDFS. In particular, it provides file-like objects
  that can be useful if a file is larger than available memory or if you know you only need part of a file.
* If you are sure you don't need any other storage APIs, or you need more direct control at the expense
  of some more difficulty porting to a new API, `hdfs` may be a better choice.

### Alluxio
Alluxio is a company and their eponymous suite of tools, which includes adapters between several kinds of
storage. Alluxio's scope is dramatically larger than LVFS and has many advantages and caveats:

* Alluxio can mount HDFS as a local volume on a Linux machine. This means every Unix command, including
  `ls` and `sqlite` and `awk` and everything else, will be able to use HDFS or S3 or many other APIs.
  This is a *really amazing concept*, and it works using the kernel's virtual file system rather than
  an interface on a user-application level.
 * There are a few open source projects trying to do this for HDFS as well, the completeness of which I
   have not been able to verify. They deserve some attention as well.
* Posix file systems and these storage APIs are so much different in the details that actually relying
  on these adapters in production is super risky, not because of anything wrong with Alluxio but on account
  of specifics. What happens when one app has a file open and another deletes it, or how `mmap()` should
  work after a process `fork()`'s? This is why bona-fide file systems take several years to develop.

LVFS is not really an alternative to Alluxio. Alluxio is a massive project connecting so many APIs to
so many others, and requires a good bit of installation and know-how. LVFS requires a single configuration
file and a few lines of Python and effects only the applications that are built for it and import it.

### Minio
Minio is a specific implementation of the Simple Storage Service protocol (S3). It provides very nice
S3 clients for several languages, including Python. It also supports an adapter server that exposes an
S3 endpoint for HDFS clusters. You could reasonably combine these to make a system where you can easily
migrate between HDFS and S3 (which many storage APIs support). This could be a good plan, but in our
case we needed to directly support GCS buckets with their SDK rather than also using S3 for
GCS, which we haven't investigated for feasibility.


Install
=======
`lvfs` is not yet published to the PyPI repository. When it is, we'll update how to install it here.
Because PyPI already has a package named `lvfs`, the package name will likely be different when it
does get published.

Configure
=========
LVFS is designed firstly for HDFS, and sometimes HDFS clusters are firewalled and in a different subnet,
requiring an intermediary host to facilitate the communnication. There are also several ways you might
need to authenticate, and there are some cases where you even need to authenticate different ways
simultaneously. LVFS handles all the common paths encountered in a large corporate environment.

LVFS configuration is stored in `~/.config/lvfs.yml` because we expect that either you are in a container,
in which case the home directory is specific to an application, or you are using a laptop, where you will
likely want to handle development keys or credentials for several projects at once.

> *Please note your company may already have documentation on how to configure LVFS*

Configuration: Connect to an unauthenticated HDFS cluster behind an SSH tunnel
------------------------------------------------------------------------------
This is probably the most questionable setup but if you find yourself using a
Hadoop cluster that does not support authentication, but is instead protected
by keeping it on a separate subnet, you will need to connect to the subnet,
and usually that is done with SSH. LVFS supports this but it is a little involved.

> If you can, avoid doing this in production. SSH tunnels are not great for
either performance or reliability.

```yaml
# LVFS only needs credentials but we prefix with this in case we need additional information later
credentials:
  # In Yaml the - indiciates this is a list element.
  # Any additional stanzas will start with a similar -
  - realm:
      # LVFS uses realms to determine which set of credentials to use for each URL.
      # Every credential stanza gets one realm.
      # LVFS will use the first stanza that matches the URL.
      # In this case we match anything using HDFSOverSSH
      classname: HDFSOverSSH
    # Note how the indentation dropped by two spaces.
    # The rest of the stanza configures the connection.
    ssh_username: your_ssh_user  # This is the username used to log into the jumpbox
                                 # LVFS will not pass SSH passwords. Use asymmetric keys instead.
    ssh_jump_host: dns_name      # This is the actual jumpbox
    # Replace this with your Hadoop username used for HDFS;
    # this may be the same as your company's SSO.
    # Do not specify a password, even an empty one. This disables authentication.
    username: ltorvalds
    # Hadoop clusters usually use Zookeeper to maintain high availability.
    # So to handle multiple master nodes, just separate the URLs with spaces.
    # You may need to ask your admin for these URLs, or they may be available on a corporate wiki or FAQ.
    webhdfs_root: http://hadoopmaster001.corporate.com:50070;http://hadoopmaster002.corporate.com:50070;http://hadoopmaster003.corporate.com:50070
```

Configuration: Connect to an unauthenticated HDFS cluster from within the subnet
--------------------------------------------------------------------------------
If you use an unauthenticated HDFS cluster, then you should avoid poking holes in
your firewalls as much as possible, and you can help do that by keeping the data
processing within the cluster. In that case, configuration is a lot easier.

```yaml
credentials:
  - realm:
      # For historic reasons, all HDFS connections use the HDFSOverSSH connection class
      # because SSH will be disabled when you don't configure it here.
      classname: HDFSOverSSH
    # This is the Hadoop username; there is no jumpbox
    username: ltorvalds
    # You may need to ask your admin for these URLs, or they may be available on a corporate wiki or FAQ.
    webhdfs_root: http://hadoopmaster001.corporate.com:50070;http://hadoopmaster002.corporate.com:50070;http://hadoopmaster003.corporate.com:50070
```

Configuration: Connect using AD credentials to an HDFS cluster with open WebHDFS access
---------------------------------------------------------------------------------------
Connecting to HDFS using authenticated, encrypted WebHDFS is generally better practice
than encrypting using SSH. LVFS is compatible with this setup and you should find it to
be quite performant. The password is stored in the clear in this file. For that reason,
*be careful to control file permissions* and be sure only to use this for development
purposes. Production applications should probably use Kerberized service principals.

```yaml
credentials:
  - realm:
      # HDFS is really always HDFSOverSSH. It's a historical artifact.
      # In this case SSH is not used.
      classname: HDFSOverSSH
    username: my_ad_username
    password: my_ad_password
    # Set this to the endpoint your cluster exposes.
    # Keep in mind, this is probably not a master node, and it probably doesn't use Zookeeper
    # Instead, this is probably a Knox server, the hint being it probably is at port 8443.
    webhdfs_root: https://theknoxserver.corporate.com:8443/gateway/default
```

Configuration: Connect using Kerberos service principals, inside an HDFS cluster
--------------------------------------------------------------------------------
This is probably the most production worthy setup, where authentication is used,
as well as encryption, but actually nothing is leaving the subnet anyway.

> This does not set up Kerberos authentication, it just uses it.
> You need to have successfully run `kinit` before running your app.

```yaml
credentials:
  - realm:
      # HDFS is really always HDFSOverSSH. It's a historical artifact.
      # In this case SSH is not used.
      classname: HDFSOverSSH
    # The username kerberos is special and will trigger Kerberos authentication
    # be sure to call `kinit` sometime before you run your app so that your
    # tickets will be set up already. LVFS will not run it for you.
    username: kerberos
    webhdfs_root: https://themasterserver.corporate.com:9871
```

Notes
-----
Kerberos still requires that you run `kinit` somehow *before* you start using LVFS,
and preferably before you even start Python. You need to figure out how to make that happen.
In an interactive terminal, `kinit` will prompt you for your AD password,
and if you have a keytab it will not. For this reason, you will need a keytab for non-interactive
logins. Your case may be different, but most likely this keytab will be in a file somewhere,
and only accessible to certain Unix users due to file permissions. Ask your admins where and who.

Finally, check with `klist` that Kerberos is already working on this machine or container.
After `kinit` has been run, check that `klist` shows the principals you expect;
If not there's no way LVFS will get it right either.

It's not clear whether it is practical to authentiate with Kerberos with SSH tunneling,
because you would need to setup Kerberos there, which may require extensive configuration.
At any rate, this is not been tested with LVFS.


For later reference, these are the possible modes for HDFSOverSSH:

ssh_jump_host | ssh_username | username | password | webhdfs_root | use case
------------- | ------------ | -------- | -------- | ------------ | --------
(any)         | (any)        | (any)    | (any)    | None         | Invalid, unconfigured
not None      | None         | (any)    | (any)    | (any)        | Invalid
None          | not None     | (any)    | (any)    | (any)        | Invalid
(any)         | (any)        | None     | not None | (any)        | Invalid
not None      | not None     | not None | None     | not None     | HDP2 with SSH
None          | None         | not None | None     | not None     | HDP2 without SSH
not None      | not None     | not None | not None | not None     | HDP3+AD with SSH
None          | None         | not None | not None | not None     | HDP3+AD without SSH
not None      | not None     | kerberos | None     | not None     | HDP3+Kerberos without SSH &
None          | None         | kerberos | None     | not None     | HDP3+Kerberos with SSH

(&) untested
  
Example code
============

## Read a text file as YAML
```py
    (URL
        .to(input("Where's the config file? "))
        .read_yaml()
    )
```

## Read parquet format tables from HDFS
You can provide a file or a directory.
Directories (as in this case) will have all their shards concatenated into one dataframe.
Partitions are not concatenated, as they would usually be too large anyway.
```py
    df = (URL
        .to("hdfs://hdfsmasternode.example.com/path/to/your/table")
        .read_parquet()
    )
```

## Copy files recursively from HDFS to local
LVFS is designed for convenience, not speed or scalability, and each file will be buffered in memory.
Don't use this method if any of your files are more than a couple gigabytes each.
You'll run out of memory if you do.

```py
    (URL
        .to("hdfs://hdfsmasternode.example.com/path/to/your/table")
        .cp("/maybe/somewhere/local")
    )
```

### Create a configurable model on the spot
Just to show how you might want to interact with URLs, here's an example where we load some
configuration files from any location (local or remote!) and then try to load a model from there.

> Keep in mind a lot of this is not about URL, it's just giving the example some context.

```py
def load_model(home: URL):
    """ Configure and read a model stored in a folder """
    composed_conf = {}
    for conf in sorted(home.ls(recursive=True)):
        # Each configuration alphabetically. Use prefixes like "05-" to compose them.
        if conf.basename().endswith(".yml"):
            # URLs are always absolute, so you can use ls() results immediately.
            composed_conf.update(conf.read_yaml())
    assert "latest_model" in composed_conf
    model = URL.to(composed_conf).read_pickle()
    return model.compose_enhanced_sheaf_cohomologies(**composed_conf)

if __name__ == "__main__":
    import os
    from argparse import ArgumentParser as A
    parser = A(description="Launch sheaf model")
    parser.add_argument("model", help="Whence come the models", default=os.getpwd())
    parser.add_argument("version", help="Model version", default="latest")
    parser.add_argument("data", help="Where the parquets are stored", default=os.getpwd())
    args = parser.parse_args()
    # You can tack on URL segments using `.join(..)`.
    # The right operand must be a string, not a URL, because URLs are absolute.
    model = load_model(URL.to(args.model).join(args.version))
    model.process(URL.to(args.data).read_parquet())
    print("All done!")
```

Architecture
============

LVFS is based on an abstract URL class with one important static method (`to(..)`).
`URL.to(..)` checks the protocol and the machine hostname to see which connection implementation
is most appropriate for the location you're running and where you want to connect to.
It will give you an instance of that implementation seamlessly.

There are a few methods that every implementation needs to implement, like `stat()`, `ls()`,
and `read_binary()` among a few others. But many methods, like `read_json()` and `cp()` are defined
in terms of the other methods in order to reduce duplication. That said, GCS is strange and there
are many sharp edges you hopefully will not be cut by. Consider for example that you must always
buffer the fill content of a GCS blob in memory, which is why LVFS does not support streaming.

API Documentation
=================
You can generate API documentation for this package after cloning it using `pdoc3` through the
`generate-docs.sh` script in the project root directory.
