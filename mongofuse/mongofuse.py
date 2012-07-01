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
        self._created = set()
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

        # Special file to filter collection
        elif fname == "query.json":
            if dirs not in self._queries:
                raise FuseOSError(errno.ENOENT)
            st['st_mode'] |= stat.S_IFREG
            st['st_size'] = len(self._queries[dirs])

        # Special file to create new documents
        elif fname == "new.json":
            st['st_mode'] |= stat.S_IFREG
            # FIXME: Report ENOENT after new.json is saved

        # Thrid level entries are documents
        elif len(components) == 4:
            doc = self._find_doc(path)
            if doc is None:
                # Entries prepared by create() call
                if path not in self._created:
                    raise FuseOSError(errno.ENOENT)
                else:
                    doc = ""

            st['st_mode'] |= stat.S_IFREG
            st['st_size'] = len(dumps(doc))

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

        if fname == "query.json":
            self._queries[dirs] = "{}"

#        # Allow creating files with names looking like objectid
        try:
            bson.objectid.ObjectId(os.path.splitext(fname)[0])
        except bson.errors.InvalidId:
            pass
        else:
            print "create objectid", path
            self._created.add(path)

        self.fd += 1
        return self.fd

    def open(self, path, flags):
        self.fd += 1
        return self.fd

    def truncate(self, path, length, fh=None):

        dirs, fname = os.path.split(path)
        
        if fname == 'query.json' and dirs in self._queries:
            self._queries[dirs] = self._queries[dirs][:length]

    def write(self, path, data, offset=0, fh=None):

        print "write", path, data

        components = split_path(path)
        dirs, fname = os.path.split(path)

        if fname == "query.json":
            self._queries[dirs] = data
            return len(data)
        
        elif len(components) > 3:
            self._save_doc(path, data)
            return len(data)

        else:
            return 0

    def unlink(self, path):

        components = split_path(path)
        if len(components) > 3:
            self._remove_doc(path)

        # TODO: Drop database
        # TODO: Drop collection

    def statfs(self, path):
        # TODO: Report real data
        return dict(f_bsize=512, f_blocks=4096, f_bavail=2048)

    def _list_documents(self, path):
        """Returns list of MongoDB documents represented as files.
        """
        
        components = split_path(path)
        db = components[1]
        coll = components[2]
        query = loads(self._queries.get(path, "{}"))

        # Database names cannot contain the character '.'
        if "." in db:
            return []

        docs = []
        for doc in self.conn[db][coll].find(query).limit(32):
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

        # Database names cannot contain the character '.'
        if "." in db:
            return None

        try:
            return self.conn[db][coll].find_one(bson.objectid.ObjectId(oid))

        except bson.errors.InvalidId:
            return None

    def _save_doc(self, path, data):
        """Saves mongo document.
        """

        components = split_path(path)
        dirs, fname = os.path.split(path)
        assert len(components) >= 4

        db = components[1]
        coll = components[2]

        doc = loads(data)

        # If document doesn't have own _id field, but named like ObjectId,
        # use that id
        if '_id' not in doc:
            try:
                doc['_id'] = bson.objectid.ObjectId(os.path.splitext(fname)[0])
            except bson.errors.InvalidId:
                pass

        self.conn[db][coll].save(doc)

    def _remove_doc(self, path):
        """Deletes mongo document. """

        components = split_path(path)
        assert len(components) >= 4

        db = components[1]
        coll = components[2]
        oid = components[-1].split(".")[0]

        try:
            self.conn[db][coll].remove(bson.objectid.ObjectId(oid))

        except bson.errors.InvalidId:
            return False

        else:
            return True



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
