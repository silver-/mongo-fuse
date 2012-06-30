# Standard modules:
import unittest
import stat

# Third-party modules:
import pymongo
import mongofuse

TEST_DB = "localhost:27017"


class RepresentDatabasesAsFoldersTest(unittest.TestCase):

    def setUp(self):
        self.conn = pymongo.Connection(TEST_DB, safe=True)
        self.fuse = mongofuse.MongoFuse(conn_string=TEST_DB)

    def test_readdir(self):

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

    def test_getattr(self):

        # Given a MongoDB database
        db = self.conn['test_1']
        db.test.insert({'db': "test_1"})

        # When getting attributes for the database folder entry
        attrs = self.fuse.getattr('/test_1')

        # Then folder flag should be set
        self.assertTrue(stat.S_ISDIR(attrs['st_mod']))


class SplitPathTest(unittest.TestCase):

    def test_should_split_path_into_list_of_components(self):

        # Given a path
        path = "/tmp/test/my_file.json"

        # When applying split_path() to it
        components = mongofuse.split_path(path)

        # Then list of individual components should be returned
        self.assertEqual(components, ["/", "tmp", "test", "my_file.json"])

    def test_should_normalize_path(self):

        # Given a non-normalized path
        path = "/tmp/test/"

        # When applying split_path() to it
        components = mongofuse.split_path(path)

        # Then list of individual components should be returned
        self.assertEqual(components, ["/", "tmp", "test"])




if __name__ == '__main__':
    unittest.main()
