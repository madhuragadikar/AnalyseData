import os
import unittest
import analyseData
import shutil, tempfile

DIR_PATH = "C:\\Users\\mgadikar\\PycharmProjects\\AnalyseData\\example"
class TestAnalyseData(unittest.TestCase):
    def setUp(self):
        self.tempDir = tempfile.mkdtemp()
        self.tempFile = "\myfile.txt"

    def tearDown(self):
        # Remove the directory after the test
        shutil.rmtree(self.tempDir)

    def test_validate_path_not_empty(self):
        f = open(self.tempDir  + self.tempFile, "a")
        f.close()
        result = analyseData.validate_path(self.tempDir)
        self.assertEqual(result[0], True)

    def test_fetch_files(self):
        f = open(self.tempDir + self.tempFile, "a")
        f.close()
        result = analyseData.fetch_files(self.tempDir)
        self.assertEqual(len(result), 1)

    def test_clean_text(self):
        text = "The owl's are not \ [] $ % &, * 123 what they seem"
        result = analyseData.clean_text(text)
        self.assertEqual(result, 'owls    seem')

if __name__ == '__main__':
        unittest.main()
