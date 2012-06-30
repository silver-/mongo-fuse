# Standard modules:
import os
import sys
import stat
import posix
import errno
import argparse

# Third-party modules:
import pymongo
from fuse import FUSE, Operations, FuseOSError

class MongoFuse(Operations):
    """File system interface for MongoDB.

    ``conn_string``
        MongoDB connection string, "host:port"

    """

    def __init__(self, conn_string):
        self.conn = pymongo.Connection(conn_string)

    def readdir(self, path, fh):

        components = split_path(path)

        # Root entry is a directory
        if len(components) == 1 and path == "/":
            return [".", ".."] + self.conn.database_names()

        # First level entries are database names
        elif len(components) == 2:
            db = components[1]
            return [".", ".."] + self.conn[db].collection_names()

        else:
            raise FuseOSError(errno.ENOENT)


    def getattr(self, path, fh=None):

        st = dict(st_atime=0,
                  st_mtime=0,
                  st_size=0,
                  st_gid=os.getgid(),
                  st_uid=os.getuid(),
                  st_mode=stat.S_IFDIR)

        components = split_path(path)

        # Root entry is a directory
        if len(components) == 1 and path == "/":
            st['st_mode'] = stat.S_IFDIR

        # First level entries are database names or collections names
        elif len(components) == 2 or len(components) == 3:
            st['st_mode'] = stat.S_IFDIR

        # Throw error for unknown entries
        else:
            raise FuseOSError(errno.ENOENT)

        return st


def split_path(path):
    """Split `path` into list of components.
    """
    
    head, tail = os.path.split(os.path.normpath(path))
    if tail:
        return split_path(head) + [tail]

    else:
        return [head]

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mount_point")
    parser.add_argument("-f", "--foreground",
                        help="Run foreground",
                        action="store_true",
                        default=True)           # TODO: Change to False
    parser.add_argument("--db",
                        help="MongoDB connection string. Default is %(default)s",
                        default="localhost:27017",
                        metavar="HOST:PORT")
    args = parser.parse_args()

    fuse = FUSE(MongoFuse(args.db),
                args.mount_point,
                foreground=args.foreground)

if __name__ == '__main__':
    main()
