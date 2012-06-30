# Standard modules:
import os
import sys
import stat
import posix
import errno

# Third-party modules:
import pymongo
from fuse import FUSE, Operations

class MongoFuse(Operations):
    """File system interface for MongoDB.

    ``conn_string``
        MongoDB connection string, "host:port"

    """

    def __init__(self, conn_string):
        self.conn = pymongo.Connection(conn_string)

    def readdir(self, path, fh):
        return [".", ".."] + self.conn.database_names()

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

        # First level entries are database names
        elif len(components) == 2:
            st['st_mode'] = stat.S_IFDIR

        # Throw error for unknown entries
        else:
            return -errno.ENOSYS


        return st



def split_path(path):
    """Split `path` into list of components.
    """
    
    head, tail = os.path.split(os.path.normpath(path))
    if tail:
        return split_path(head) + [tail]

    else:
        return [head]


if __name__ == '__main__':
    if len(sys.argv) != 2:
        print "Usage: %s <mount-point>" % sys.argv[0]
        sys.exit(1)

    fuse = FUSE(MongoFuse("localhost:27017"), sys.argv[1], foreground=True)
