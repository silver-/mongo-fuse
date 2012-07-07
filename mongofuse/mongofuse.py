# Standard modules:
import os
import sys
import stat
import posix
import errno
import argparse
import json
import collections
import time

# Third-party modules:
import pymongo
import bson
import bson.json_util
from fuse import FUSE, Operations, FuseOSError, LoggingMixIn


class MongoFuse(LoggingMixIn, Operations):
    """File system interface for MongoDB.

    ``conn_string``
        MongoDB connection string, "host:port"

    """

    def __init__(self, conn_string):
        self.conn = pymongo.Connection(conn_string)
        self._queries = {}                            # path => query_content
        self._created = set()
        self._dirs = collections.defaultdict(set)     # path => {subdirs}
        self.fd = 0
        self.attrs_cache = LRUCache(expire_secs=2)

    def readdir(self, path, fh=None):


        components = split_path(path)
        dirs, fname = os.path.split(path)

        # Root entries are database names
        if len(components) == 1 and path == "/":
            return [".", ".."] + self.conn.database_names()

        # Second level entries are collection names
        elif len(components) == 2:
            db = components[1]
            return [".", ".."] + self.conn[db].collection_names()

        # Third and more level entries are mongo documents and user subfolders
        elif len(components) >= 3:
            files = [".", ".."] + \
                    self._list_documents(path) + \
                    list(self._dirs.get(path, []))
            if path in self._queries:
                files += ['query.json']
            return files

        else:
            raise FuseOSError(errno.ENOENT)

    def getattr(self, path, fh=None):


        st = dict(st_atime=0,
                  st_mtime=0,
                  st_size=0,
                  st_gid=os.getgid(),
                  st_uid=os.getuid(),
                  st_mode=0770)

        components = split_path(path)
        dirs, fname = os.path.split(path)

        # Root entry is a directory
        if len(components) == 1 and path == "/":
            st['st_mode'] |= stat.S_IFDIR

        # First level entry maybe a database name
        elif len(components) == 2 and components[-1] in self.conn.database_names():
            st['st_mode'] |= stat.S_IFDIR

        # Second level entry maybe a collection name
        elif len(components) == 3 and \
                (components[-1] in self.conn[components[1]].collection_names()):
            st['st_mode'] |= stat.S_IFDIR

        # User-created folders
        elif fname in self._dirs.get(dirs, []):
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

        # Thrid and more level entries are documents
        elif len(components) >= 4:
            cached = self.attrs_cache.get(fname)
            if cached:
                return cached

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
        return ''

    def read(self, path, size, offset=0, fh=None):


        components = split_path(path)
        dirs, fname = os.path.split(path)

        if fname == "query.json" and dirs in self._queries:
            return self._queries[dirs]

        if len(components) >= 4:
            doc = self._find_doc(path)
            if doc is None:
                raise FuseOSError(errno.ENOENT)
            else:
                return dumps(doc)

    def create(self, path, mode):

        dirs, fname = os.path.split(path)

        if fname == "query.json":
            self._queries[dirs] = "{}"

#        # Allow creating files with names looking like objectid
        try:
            bson.objectid.ObjectId(os.path.splitext(fname)[0])
        except bson.errors.InvalidId:
            pass
        else:
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
        # TODO: Accurate delete of collection views

    def mkdir(self, path, mode):

        components = split_path(path)
        dirs, dirname = os.path.split(path)

        if dirs == "/" and not dirname in self.conn.database_names():
            self.conn[dirname].create_collection("system.indexes")

        elif len(components) == 3:
            db = components[1]
            coll = components[2]
            self.conn[db].create_collection(coll)

        self._dirs[dirs].add(dirname)


    def chmod(self, path, mode):
        return 0
    
    def chown(self, path, uid, gid):
        pass

    def statfs(self, path):
        # TODO: Report real data
        return dict(f_bsize=512, f_blocks=4096*1024, f_bavail=2048*1024)

    def _list_documents(self, path):
        """Returns list of MongoDB documents represented as files.
        """

        components = split_path(path)
        db = components[1]
        coll = components[2]
        query = loads(self._queries.get(path, "{}"))
        st = dict(st_atime=0,
                  st_mtime=0,
                  st_size=0,
                  st_gid=os.getgid(),
                  st_uid=os.getuid(),
                  st_mode=0770 | stat.S_IFREG)

        # Database names cannot contain the character '.'
        if "." in db:
            return []

        docs = []
        for doc in self.conn[db][coll].find(query).limit(32):
            fname = "{}.json".format(doc["_id"])
            docs.append(fname)

            # Cache doc attributes
            attrs = st.copy()
            attrs['st_size'] = len(dumps(doc))
            self.attrs_cache[fname] = attrs

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


class LRUCache(dict):
    """Simple Least Recently Used (LRU) cache.

    Removes contained items after `expire_secs` seconds.

    """

    def __init__(self, expire_secs=2):
        self.expire_secs = expire_secs
        self._time_added = {}

    def __setitem__(self, key, value):
        self._delete_expired()
        self._time_added[key] = time.time()
        dict.__setitem__(self, key, value)

    def __getitem__(self, key):
        self._delete_expired()
        return dict.__getitem__(self, key)

    def __contains__(self, key):
        self._delete_expired()
        return dict.__contains__(self, key)

    def __len__(self):
        self._delete_expired()
        return dict.__len__(self)
    
    def _delete_expired(self):
        now = time.time()
        for key, added in self._time_added.items():
            if now - added > self.expire_secs:
                del self[key]
                del self._time_added[key]


def split_path(path):
    """Split `path` into list of components.
    """
    
    head, tail = os.path.split(os.path.normpath(path))
    if tail:
        return split_path(head) + [tail]

    else:
        return [head]

def dumps(doc):

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
                        default=False)
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
