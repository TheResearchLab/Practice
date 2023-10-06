from statefile import State
import os
import shutil 
import tempfile 
import unittest

INITIAL_STATE = '{"foo":42,"bar":17}'

class TestState(unittest.TestCase):
    def setUp(self):
        self.testdir = tempfile.mkdtemp()
        self.state_file_path = os.path.join(self.testdir,'statefile.json')
        with open(self.state_file_path,'w') as outfile:
            outfile.write(INITIAL_STATE)
        self.state = State(self.state_file_path)
    
    def tearDown(self):
        shutil.rmtree(self.testdir)

    def test_change_value(self):
        self.state.data["foo"] = 21
        self.state.close()
        reloaded_statefile = State(self.state_file_path)
        self.assertEqual(21,reloaded_statefile.data["foo"])
