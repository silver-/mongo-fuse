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
        return self.conn.database_names()



