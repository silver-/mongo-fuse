# Standard modules:
import os
import sys
import stat
import posix
import errno
import argparse
import json

# Third-party modules:
import pymongo
import bson
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

        # Third level entries are mongo documents
        elif len(components) == 3:
            _, db, coll = components
            return [".", ".."] + self._list_documents(db, coll)

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

        # Thrid level entries are documents
        elif len(components) == 4:
            st['st_mode'] = stat.S_IFREG
            st['st_size'] = 4096

        # Throw error for unknown entries
        else:
            raise FuseOSError(errno.ENOENT)

        return st
    
    def read(self, path, size, offset, fh):

        components = split_path(path)

        if len(components) == 4:
            doc = self._find_doc(path)
            if doc is None:
                return "{}"

            del doc['_id']
            return dumps(doc)


    def _list_documents(self, db, coll):
        """Returns list of MongoDB documents represented as files.
        """
        
        if "." in db:
            return []

        docs = []
        for doc in self.conn[db][coll].find().limit(10):
            docs.append("{}.json".format(doc["_id"]))

        return docs

    def _find_doc(self, path):
        """Return mongo document found by given `path`.
        """

        components = split_path(path)
        assert len(components) >= 4

        db = components[1]
        coll = components[2]
        oid = components[-1].split(".")[0]

        return self.conn[db][coll].find_one(bson.objectid.ObjectId(oid))


def split_path(path):
    """Split `path` into list of components.
    """
    
    head, tail = os.path.split(os.path.normpath(path))
    if tail:
        return split_path(head) + [tail]

    else:
        return [head]

def dumps(doc):
    """Returns pretty-printed `doc`. """

    return json.dumps(doc, indent=4)






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
