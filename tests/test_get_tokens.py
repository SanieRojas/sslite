'''testing the dataframe processing functions'''
import unittest
import pandas as pd
from dags.functions import get_tokens


class TestGetTokens(unittest.TestCase):
    ''' tests Get_Tokens function '''
    def test_empty_input(self):
        ''' tests result continuance when input is empty '''
        result = get_tokens('')
        self.assertEqual(result, [])

    def test_single_word_input(self):
        ''' tests lower casing'''
        result = get_tokens('Hello')
        self.assertEqual(result, ['hello'])

    def test_punctuation_removal(self):
        ''' tests split and punctuation removal'''
        result = get_tokens('Hello, world!')
        self.assertEqual(result, ['hello', 'world'])

    def test_stopwords_removal(self):
        ''' tests removal of stopwords'''
        result = get_tokens('The quick brown fox')
        self.assertEqual(result, ['quick', 'brown', 'fox'])


if __name__ == '__main__':
    unittest.main()