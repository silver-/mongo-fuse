import unittest
import pymongo
import mongofuse

TEST_DB_HOST = "localhost:27017"


class MongoFuseTest(unittest.TestCase):

    def setUp(self):
        self.conn = pymongo.Connection(TEST_DB_HOST)

    def test_should_represent_databases_as_folders(self):

        # Given number of MongoDB databases
        db1 = self.conn['test_1']
        db2 = self.conn['test_2']
        db3 = self.conn['test_3']

        db1.test.insert({"db": "test_1"})
        db2.test.insert({"db": "test_2"})
        db3.test.insert({"db": "test_3"})

        # When listing files in root dir
        listdir = mongofuse.MongoFuse().listdir('/')

        # Then database names should appear in listed files
        self.assertIn('test_1', listdir)
        self.assertIn('test_2', listdir)
        self.assertIn('test_3', listdir)


if __name__ == '__main__':
    unittest.main()
