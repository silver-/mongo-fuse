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
import bson.json_util
from fuse import FUSE, Operations, FuseOSError

class MongoFuse(Operations):
    """File system interface for MongoDB.

    ``conn_string``
        MongoDB connection string, "host:port"

    """

    def __init__(self, conn_string):
        self.conn = pymongo.Connection(conn_string)
        self._queries = {}
        self.fd = 0

    def readdir(self, path, fh=None):

        print "readdir", path

        components = split_path(path)
        dirs, fname = os.path.split(path)

        # Root entry is a directory
        if len(components) == 1 and path == "/":
            return [".", ".."] + self.conn.database_names()

        # First level entries are database names
        elif len(components) == 2:
            db = components[1]
            return [".", ".."] + self.conn[db].collection_names()

        # Third level entries are mongo documents
        elif len(components) == 3:
            files = [".", ".."] + self._list_documents(path)
            if path in self._queries:
                files += ['query.json']
            return files

        else:
            raise FuseOSError(errno.ENOENT)

    def getattr(self, path, fh=None):

        print "getattr", path

        st = dict(st_atime=0,
                  st_mtime=0,
                  st_size=0,
                  st_gid=os.getgid(),
                  st_uid=os.getuid(),
                  st_mode=0755)

        components = split_path(path)
        dirs, fname = os.path.split(path)

        # Root entry is a directory
        if len(components) == 1 and path == "/":
            st['st_mode'] |= stat.S_IFDIR

        # First level entries are database names or collections names
        elif len(components) == 2 or len(components) == 3:
            st['st_mode'] |= stat.S_IFDIR

        elif fname == "query.json":
            if dirs not in self._queries:
                raise FuseOSError(errno.ENOENT)
            st['st_mode'] |= stat.S_IFREG
            st['st_size'] = len(self._queries[dirs])

        # Thrid level entries are documents
        elif len(components) == 4:
            st['st_mode'] |= stat.S_IFREG
            st['st_size'] = len(dumps(self._find_doc(path)))


        # Throw error for unknown entries
        else:
            raise FuseOSError(errno.ENOENT)

        return st
    
    def getxattr(self, path, name, position=0):
        print "getxattr", path
        return ''

    def read(self, path, size, offset=0, fh=None):

        print "read", path

        components = split_path(path)
        dirs, fname = os.path.split(path)

        if fname == "query.json" and dirs in self._queries:
            print "READ QUERY"
            return self._queries[dirs]

        if len(components) == 4:
            doc = self._find_doc(path)
            if doc is None:
                raise FuseOSError(errno.ENOENT)
            else:
                return dumps(doc)

    def create(self, path, mode):

        print "Create", path
        dirs, fname = os.path.split(path)
        self._queries[dirs] = "{}"
        self.fd += 1
        return self.fd

    def open(self, path, flags):
        self.fd += 1
        return self.fd

    def truncate(self, path, length, fh=None):

        dirs, fname = os.path.split(path)
        
        if fname == 'query.json' and dirs in self._queries:
            self._queries[dirs] = self._queries[dirs][:length]

    def write(self, path, data, offset, fh):

        print "write", path, data

        dirs, fname = os.path.split(path)
        if fname == "query.json":
            self._queries[dirs] = data
            return len(data)
        
        else:
            return 0

    def statfs(self, path):
        # TODO: Report real data
        return dict(f_bsize=512, f_blocks=4096, f_bavail=2048)

    def _list_documents(self, path):
        """Returns list of MongoDB documents represented as files.
        """
        
        # FIXME: Need only check for special ./.. folders in path
        if "." in path:
            return []

        components = split_path(path)
        db = components[1]
        coll = components[2]
        query = loads(self._queries.get(path, "{}"))

        docs = []
        for doc in self.conn[db][coll].find(query).limit(10):
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

        try:
            return self.conn[db][coll].find_one(bson.objectid.ObjectId(oid))

        except bson.errors.InvalidId:
            return None


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

    return json.dumps(doc,
                      indent=4,
                      sort_keys=True,
                      default=bson.json_util.default)

def loads(string):
    """Returns document parsed from `string`. """

    return json.loads(string, object_hook=bson.json_util.object_hook)

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
