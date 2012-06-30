import unittest
import pymongo
import mongofuse

TEST_DB = "localhost:27017"


class MongoFuseTest(unittest.TestCase):

    def setUp(self):
        self.conn = pymongo.Connection(TEST_DB)
        self.fuse = mongofuse.MongoFuse(conn_string=TEST_DB)

    def test_should_represent_databases_as_folders(self):

        # Given number of MongoDB databases
        db1 = self.conn['test_1']
        db2 = self.conn['test_2']
        db3 = self.conn['test_3']

        self.addCleanup(self.conn.drop_database, 'test_1')
        self.addCleanup(self.conn.drop_database, 'test_2')
        self.addCleanup(self.conn.drop_database, 'test_3')

        db1.test.insert({"db": "test_1"})
        db2.test.insert({"db": "test_2"})
        db3.test.insert({"db": "test_3"})

        # When listing files in root dir
        readdir = self.fuse.readdir('/', fh=None)

        # Then database names should appear in listed files
        self.assertIn('test_1', readdir)
        self.assertIn('test_2', readdir)
        self.assertIn('test_3', readdir)

        # And special "." and ".." folders should be listed as well
        self.assertIn(".", readdir)
        self.assertIn("..", readdir)


if __name__ == '__main__':
    unittest.main()
