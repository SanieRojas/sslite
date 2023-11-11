'''testing the dataframe processing functions'''
import unittest
import pandas as pd
from dags import get_tokens, get_scores  # Replace


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


class TestGetScores(unittest.TestCase):
    ''' tests Get_Scores function '''
    #def test_empty_dataframe(self):
    #''' checks weather it will work with an empty dataframe'''
        #empty_df = pd.DataFrame()
        #result = get_scores(empty_df)
        #self.assertTrue(result.empty)
        #self.assertTrue("score" in result.columns)
        #self.assertEqual(len(result), 0)

    def test_non_empty_dataframe(self):
        ''' checks get_score function in a non-empty test database'''    
        data = {
            "tokens": [["good", "example"], ["bad", "example"]],
        }
        input_df = pd.DataFrame(data)
        result = get_scores(input_df)
        self.assertTrue("score" in result.columns)
        self.assertEqual(len(result), len(input_df))

if __name__ == '__main__':
    unittest.main()