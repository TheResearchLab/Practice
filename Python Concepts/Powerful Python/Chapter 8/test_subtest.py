import unittest 
from subtest import subtest_func

class TestSubtest(unittest.TestCase):
    def test_whitespace_subtest(self):
        texts = ['foo bar',' foobar',' foo bar']
        
        for text in texts:
            with self.subTest(text=text):
                self.assertEqual(2,subtest_func(text))
