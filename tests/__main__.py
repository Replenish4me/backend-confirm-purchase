import unittest

if __name__ == '__main__':
    test_suite = unittest.defaultTestLoader.discover('./tests', pattern='test*.py')
    test_runner = unittest.TextTestRunner()
    test_runner.run(test_suite)
