import re
from urllib.parse import unquote
import spacy
import numpy as np
from concurrent.futures import ProcessPoolExecutor
from tqdm import tqdm
import pandas as pd

# load stop words for specified languages
from spacy.lang.en.stop_words import STOP_WORDS as en_stop
from spacy.lang.es.stop_words import STOP_WORDS as es_stop
from spacy.lang.fr.stop_words import STOP_WORDS as fr_stop
from spacy.lang.it.stop_words import STOP_WORDS as it_stop
from spacy.lang.de.stop_words import STOP_WORDS as de_stop

stop_words_dict = {
    'en': en_stop,
    'es': es_stop,
    'de': de_stop,
    'fr': fr_stop,
    'it': it_stop
}

# Load nlp model once for efficiency
nlp = spacy.load('en_core_web_sm')


# Function to load stop words for specified languages
def load_stop_words(languages):
    stop_words = set()
    for lang in languages:
        if lang not in stop_words_dict:
            continue
        stop_words = stop_words.union(stop_words_dict[lang])
    return stop_words


# Function to preprocess text
def preprocess_text(text):
    text = re.sub(r'\s+', ' ', text)  # Remove newlines and multiple spaces
    text = text.lower()
    text = re.sub(r'http\S+', '', text)  # Remove links
    text = re.sub(r'[^\w\süáñç]', '', text)  # Remove everything except words, spaces, and specific characters
    return text


# Main text processing function (now accepting languages)
def process_text(text, languages):
    languages = [lang for lang in languages if isinstance(lang, str)]
    stop_words = load_stop_words(languages)
    
    # Preprocess the text
    text = preprocess_text(text)
    
    # Tokenize with Spacy (we'll use the English model for tokenization purposes)
    doc = nlp(text)
    
    # Remove stop words (from all specified languages) and other criteria
    tokens = [token.lemma_ for token in doc if token.text not in stop_words and not token.is_punct and not token.like_num and len(token.text) > 1]
    return " ".join(tokens)


def process_wikipedia_tags(tags):
    """
    Processes a list of Wikipedia tag URLs by decoding, replacing underscores with
    spaces, and concatenating them into a single string.

    Args:
        tags (list): A list of Wikipedia tag URLs.

    Returns:
        str: A single string of processed and concatenated Wikipedia tags.
    """
    # Processes each tag URL, decoding and replacing underscores with spaces.
    processed_tags = [unquote(tag).split(
        '/')[-1].replace('_', ' ') for tag in tags]
    # Concatenates the processed tags into a single string.
    return ' '.join(processed_tags)


def parallelize_dataframe_processing(df, func, n_cores):
    # Split DataFrame into chunks
    df_split = np.array_split(df, n_cores)
    # Create a pool of processes
    pool = ProcessPoolExecutor(n_cores)
    
    # Process the DataFrame chunks in parallel
    df_processed_list = list(tqdm(pool.map(func, df_split), total=n_cores))
    # Combine chunks back into a single DataFrame
    df_processed = pd.concat(df_processed_list, ignore_index=True)

    return df_processed


def process_chunk(chunk):
    # Process each chunk with the languages
    chunk['processed_text'] = chunk.apply(lambda row: process_text(row['combined_text'], row['languages']), axis=1)
    return chunk
