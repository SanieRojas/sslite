"""Functions to setup the instance of a google news search engine, define its period time frame,
select subjects to monitor, retrive the results and store them in several files, one by subject, 
 which will later be compiled in sub_process_accumulate."""

from datetime import datetime
import json
import re

import nltk
import pandas as pd

from GoogleNews import GoogleNews
from nltk import ne_chunk, pos_tag
from nltk.corpus import stopwords
from nltk.sentiment.vader import SentimentIntensityAnalyzer
from nltk.tokenize import word_tokenize

from a_setup import topics

nltk.download(['stopwords', 'vader_lexicon', 'punkt'], quiet=True) 
nltk.download('maxent_ne_chunker', quiet=True)
nltk.download('words', quiet=True)
nltk.download('averaged_perceptron_tagger', quiet=True)


def setup_engine(period, subject, monitor):
    """
    Set up the google engine to retrieve news of a given period.  Performs a search of news for any selected subject theme and, 
    if prompted, saves it into a database. 
    Documentation -> https://pypi.org/project/GoogleNews/

    :period: defines how much time to search news for. 
    :type: 7d [quantity of days + "d"] 

    :monitor: Yes if you want to keep your results. 
    :type: Boolean

    :subject: Subject of interest
    :rtype: String

    :return: Newsfeed dataframe, log date 
    :rtype: pandas dataframe, date   
    """
    # Set up date
    id = str(datetime.now().timestamp())[:10]
    time_stamp = datetime.now().timestamp()
    log_date = datetime.fromtimestamp(time_stamp) #CHANGE LATER TO CONTEXT ds

    # Quick off instance
    api = GoogleNews()
    api.set_lang("en")
    api.set_encode("utf-8")
    api.set_period(period)
    api.get_news(subject)
    results = api.results(sort=True)

    #Save to dataframe 
    newsfeed = pd.DataFrame(results)
    newsfeed["log_date"] = time_stamp
    newsfeed["subject"] = subject

    #If selected for monitor, save to file
    if monitor is True:
        
        file_name = f'files/raw/raw_{subject}_{id}.csv'
        newsfeed.to_csv(file_name, index=False)

    else:
        pass

    # Returns a pandas dataframe with the newsfeed gathered and the log_date
    return newsfeed, log_date, time_stamp, subject

def data_cleanse(newsfeed):
    """ Remove unused columns."""
    df = newsfeed.drop(columns=['desc','site','link','img','media','log_date'], axis=1)
    return df

def get_tokens(newsfeed):
    """ Clean text."""
    my_stopwords = nltk.corpus.stopwords.words("english")
    cleaned_text = re.sub(r'[^a-zA-Z\s]', '', newsfeed).lower()

    words = nltk.word_tokenize(cleaned_text)
    tokens = [word for word in words if word not in my_stopwords]

    return tokens

def get_scores(df):
    """ Get scores."""
    scores = []
    analyzer = SentimentIntensityAnalyzer()
    for i in range(len(df)):
        tokens = df["tokens"][i]
        sentiment_score = analyzer.polarity_scores(' '.join(tokens))['compound']
        scores.append(sentiment_score)
    df["score"] = scores
    return df

def extract_entities(txt):
    """ extract main entities."""
    with open(txt, 'r',  encoding='utf-8') as f:
        text = f.read()
    entities = {}
    for sent in nltk.sent_tokenize(text):
        for chunk in nltk.ne_chunk(nltk.pos_tag(nltk.word_tokenize(sent))):
            if hasattr(chunk, 'label'):
                entity = ' '.join(c[0] for c in chunk)
                entities[entity] = entities.get(entity, 0) + 1
    return entities

########################################

dataframes = []
entities = []
#call the function for each selected topics 

for topic in topics:
    step1 = setup_engine("1h", topic, monitor=True)
    step2 = data_cleanse(step1[0])
    step2["tokens"] = step2["title"].apply(get_tokens)
    step3 = get_scores(step2)  ## Final dataframe for analysis
    #step3.to_csv(f'{topic}_processed.csv', header=True, index=False, sep= "\t")
    dataframes.append(step3)


    #####
    step4 = step3["title"].to_csv(f'files/{topic}{step1[2]}.txt', header=True, index=False, sep= "\t")
    text_file = f'files/{topic}{step1[2]}.txt'
    step5 = extract_entities(text_file)
    step6 = sorted(step5.items(), key=lambda item: item[1], reverse=True) ## Final entities dataframe
    step7 = pd.DataFrame(step6, columns=['Item', 'Count'])

    entities.append(step7)
    


compiled_data = pd.concat(dataframes, ignore_index=True)
compiled_data.to_csv(f'files/{step1[2]}_compiled.csv', header=True, index=False, sep= ",")
compiled_entities = pd.concat(entities)
compiled_entities = compiled_entities.to_csv(f'files/{step1[2]}_entities.csv', header=True, index=False, sep= ",")





