import os
import glob
import re
import string
import nltk
from nltk.tokenize import word_tokenize, sent_tokenize
from nltk.probability import FreqDist
from nltk.corpus import stopwords
from nltk.stem.snowball import SnowballStemmer
import sqlite3
from collections import defaultdict, Counter

DIR_PATH ="C:\\Users\\mgadikar\\PycharmProjects\\AnalyseData\\example"
DEFAULT_EXT='\*.*'
MOST_COMMON_NUM = 5
conn = sqlite3.connect('example.db')
c = conn.cursor()

def process_data(**kwargs):
    """
    Print most common interesting words and the sentences that contain those words
    :param **kwargs: path - Directory where the test files are stored
    :param ext: File extension to process. Default '*.* if ext not specified
    TODO: Output needs to be formatted.
    Currently, data is displayed straight from the database
    """
    extra_args = {}
    for key, value in kwargs.items():
        extra_args[key] = value

    path = extra_args.get('path', None)
    is_valid, error = validate_path(path)

    if is_valid:
        if ((extra_args.get('ext', None)) is not None):
            path += extra_args.get('ext')
        else:
            path += DEFAULT_EXT

        my_file_list = fetch_files(path)

        # Process further if there is atleast one file to process
        if my_file_list:
            #Create empty table in db
            c.execute("DROP TABLE IF EXISTS data")
            c.execute("CREATE TABLE data (name TEXT, counter INTEGER, file TEXT, sentence TEXT)")
            conn.commit()

            # Analyse data for the files specified
            analyse_data(my_file_list)

            c.execute("SELECT name, sum(counter)as counter, GROUP_CONCAT(DISTINCT file) as file, sentence FROM data GROUP BY name")
            result = c.fetchall()
            print(result)
            conn.close()
        else:
            print(f"Error: Directory {path} is either empty or there are no files with the specified extension!")
    else:
        print(f'Error: {error}')


def validate_path(path):
    """
    Perform validation to check if:
    1. Directory path is specified and exists
    2. Path specified is a directory
    3. Directory not empty
    :param path: Directory path
    :return: Boolean valid_path, String error
    """
    valid_path = True
    error = ''

    # Check if path specified and exists
    if not path or not os.path.exists(path):
        valid_path = False
        error = 'Directory path incorrect'
    elif not os.path.isdir(path):
        valid_path = False
        error = 'Please specify directory path'
    elif not os.listdir(path):
        valid_path = False
        error = f'Directory {path} is empty'

    return valid_path, error


def fetch_files(path):
    if path:
        return [f for f in glob.glob(path)]

def analyse_data(my_files):
    """
    This function
    1. Builds tuples in the format,
    (top word, counter, file name, sentences containing the top word)
    2. Stores this tuple data in the sqlite3 db.
    :param my_files: list of file names (with full path)
    """
    updated_tuples = tuple()
    # Get top most words and count from all the files specified
    top_word_tuple = get_tokens(my_files)

    # Update the top most words and count with the respective file names and sentences
    updated_tuples = update_with_file_and_sentences(top_word_tuple, my_files)

    # Avoid exception in case tuple returned without updating file and/or sentence information
    # Example: in case of countri tuple returned is (countri, 10)
    try:
        c.executemany("INSERT INTO data VALUES (?, ?, ?, ?)", updated_tuples)
        conn.commit()
    except:
        print("Error updating database")


def get_tokens(files):
    """
    This function tokenizes and stems the data; builds tokens and
    counters based on the data after removing punctuation and
    commonly used stop words
    :param  file: file name to be processed
    :return top_words_list: List of top words
    """
    top_words_dict = defaultdict(int)
    top_words_list = []
    for file in files:
        if os.path.isfile(file):
            # Clean up data - remove punctuation and stop words
            # Build token list with most common words - based on MOST_COMMON_NUM

            # Possibly use a better Stemmer/Lemmitiser as currently words like country
            # changes to countri and hence not populated with any file names and sentences
            stemmer = SnowballStemmer('english')
            clean_data = ''
            with open(file, "r") as my_file:
                for line in my_file.readlines():
                    clean_data += clean_text(line)
            my_file.close()
            tokens = word_tokenize(clean_data)
            stem_tokens = [stemmer.stem(token) for token in tokens]
            fdist = FreqDist(stem_tokens)
            top_word_tuple = fdist.most_common(MOST_COMMON_NUM)
            top_words_list += top_word_tuple

    # Combine all top words from each file and compute the most common across all files
    for name, counter in top_words_list:
        top_words_dict[name] += counter

    Keymax = Counter(top_words_dict)
    # Returns top most common words in the format [('word1', 10), ('word2', 6), () .... ]
    top_words_list = Keymax.most_common(MOST_COMMON_NUM)

    return top_words_list

def update_with_file_and_sentences(top_word_tuple, files):
    """
    This function updates the too_word_tuple with file name and
    sentences containing the top word.
    :param top_word_tuple: tuple containing top word and counter
    format: (('TopWord', 20),.... )
    :param file: list of file names to be processed
    :return top_word_list: list of tuples containing top word, counter, file name and sentence
    format: [['TopWord', 20, 'file_name', 'This is a sentence'], .... ]
    """
    word_dict = defaultdict(lambda: [0, defaultdict(set), defaultdict(list)])
    top_word_list = []
    for file in files:
        file_path, file_name = os.path.split(file)
        with open(file, "r") as my_file:
            file_data = my_file.read()
            for sentence in sent_tokenize(file_data):
                # Look for each top word in all the sentences
                for top_word in top_word_tuple:
                    if re.search(top_word[0], sentence, re.IGNORECASE):
                        word_dict[top_word[0]][0] = top_word[1]
                        word_dict[top_word[0]][1]['file'].add(file_name)
                        word_dict[top_word[0]][2]['sentence'].append(sentence)

    # Update tuple to list
    for item in top_word_tuple:
        top_word_list.append(list(item))

    # Traverse through the dict items to update file and sentence details
    for word, contents in word_dict.items():
        #Update top_word_list with file names
        for file_key, file_value in contents[1].items():
            for sublist in top_word_list:
                if sublist[0] == word:
                    sublist.append(', '.join(file_value))

        #Update top_word_list with sentences
        for sentence_key, sentence_value in contents[2].items():
            for sublist in top_word_list:
                if sublist[0] == word:
                    sublist.append(', '.join(sentence_value))

    return top_word_list

def clean_text(text):
    """
    This function -
    1. changes the text to lower case
    2. removes the specified punctuation
    3. removes stopwords
    4. Removes numbers
    :param text: original text from file
    :return text: Cleaned up text
    """
    stop_words = stopwords.words('english')
    stop_words.extend(string.punctuation)
    stop_words.append('')
    text = text.lower()
    text =  " ".join(filter(lambda word: word not in stop_words, text.split()))
    text = re.sub('[%s]' % re.escape(string.punctuation), '', text)
    text = re.sub('\w*\d\w*', '', text)
    return text

if __name__ == '__main__':
    process_data(path=DIR_PATH, ext='\*.txt')