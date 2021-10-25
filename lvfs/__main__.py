# Command line interface for URL
import asyncio
from argparse import ArgumentParser
from lvfs.url import URL

parser = ArgumentParser(
    "lvfs", description="Local and remote file management for humans"
)
parser.set_defaults(command=lambda _: parser.print_help())
subs = parser.add_subparsers()


async def ls(args):
    """ ls command

        Accepts
        -------
        * args: a Namespace as returned by argparse.ArgumentParser.parse_args
    """
    async for u in URL.to(args.url).ls():
        print(u.raw)


sub = subs.add_parser("ls")
sub.set_defaults(command=ls)
sub.add_argument("url", help="What folder to list")


async def cp(args):
    """ cp command

        Accepts
        -------
        * args: a Namespace as returned by argparse.ArgumentParser.parse_args
    """
    await URL.to(args.source).cp(args.dest, recursive=args.recursive)


sub = subs.add_parser("cp")
sub.set_defaults(
    command=cp, description="Move files or folders, optionally recursively"
)
sub.add_argument("source", help="Source file or folder")
sub.add_argument("dest", help="Destination file or folder")
sub.add_argument(
    "-r", "--recursive", action="store_true", help="Whether to operate recursively"
)


async def mv(args):
    """ mv command

        Accepts
        -------
        * args: a Namespace as returned by argparse.ArgumentParser.parse_args
    """
    await URL.to(args.source).mv(args.dest)


sub = subs.add_parser("mv", description="Move files or folders, always recursively")
sub.set_defaults(command=mv)
sub.add_argument("source", help="Source file or folder")
sub.add_argument("dest", help="Destination file or folder")


async def rm(args):
    """ rm command

        Accepts
        -------
        * args: a Namespace as returned by argparse.ArgumentParser.parse_args
    """
    await URL.to(args.url).rm()


sub = subs.add_parser(
    "rm", description="Remove files or folders, optionally recursively"
)
sub.set_defaults(command=rm)
sub.add_argument("url", help="File or folder to remove")


async def mkdir(args):
    """ mkdir command

        Accepts
        -------
        * args: a Namespace as returned by argparse.ArgumentParser.parse_args
    """
    await URL.to(args.url).mkdir()


sub = subs.add_parser("mkdir", description="Create a new empty directory")
sub.set_defaults(command=mkdir)
sub.add_argument("url", help="File or folder to create")

all_args = parser.parse_args()
asyncio.run(all_args.command(all_args))
