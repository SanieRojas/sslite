'''testing the dataframe processing functions'''
import unittest
import pandas as pd
from dags.functions import get_scores  


class TestGetScores(unittest.TestCase):
    ''' tests Get_Scores function '''
    nltk.download(['stopwords', 'vader_lexicon', 'punkt'], quiet=True) 
    nltk.download('maxent_ne_chunker', quiet=True)
    nltk.download('words', quiet=True)
    nltk.download('averaged_perceptron_tagger', quiet=True)
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