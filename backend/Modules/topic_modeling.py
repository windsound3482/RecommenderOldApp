import sys
sys.path.append('/Users/pablojerezarnau/git/RS-backend/')
from Modules import database_queries
from Modules import text_processing
import pandas as pd
import os
from gensim import corpora
import gensim
import json
from config.settings import TOPIC_MODELING_DATA_DIR
from tqdm import tqdm
tqdm.pandas(desc="Processing texts")


def save_run_parameters(run_dir, params):
    """
    Saves the parameters used in a topic modeling run to a JSON file within the run's directory.

    Parameters:
    - run_dir (str): The directory path for the run.
    - params (dict): The parameters dictionary to be saved.
    """
    params_file_path = os.path.join(run_dir, 'parameters.json')

    with open(params_file_path, 'w') as params_file:
        json.dump(params, params_file, indent=4)

    print(f"Run parameters saved to {params_file_path}.")


def get_languages(row):
    """
    Identify the languages present in the the language fields.
    """
    languages = set()
    for field in ['defaultLanguage', 'defaultAudioLanguage', 'predictedLanguage']:
        lang = row[field]
        if isinstance(lang, str):
            if lang.startswith('en') or lang in ['es', 'fr', 'it', 'de']:
                languages.add(lang[:2])  # Only use the first two characters for language code
    return list(languages)


def get_textual_features(data_path, force_overwrite=False):
    """
    Use function from database_queries to get all the data instances. Create a pandas dataframe
    where the rows are videos and the columns are 'title', 'description', 'tags' and 'wikipedia_tags'.
    If the dataframe is already stored locally, read it from there to save time.
    """

    if os.path.isfile(data_path) and not force_overwrite:
        try:
            # Ensure lines=True is used to match the file format
            textual_features_df = pd.read_json(data_path, lines=True)
            print("Loaded data from local file.")
        except ValueError as e:
            print(f"Error reading the JSON file: {e}")
            # Handle the error or fallback to regenerating the DataFrame
    else:
        print("Local file not found, querying the database...")
        all_data_instances = database_queries.get_entire_database()
        textual_features_df = pd.DataFrame.from_records([
            {
                'id': video_data.get('id', ''),
                'title': video_data.get('title', ''),
                'description': video_data.get('description', ''),
                'tags': video_data.get('tags', []),
                'wikipedia_tags': video_data.get('topicCategories', []),
                'link': video_data.get('link', []),
                'defaultLanguage': video_data.get('defaultLanguage', []),
                'defaultAudioLanguage': video_data.get('defaultAudioLanguage', []),
                'predictedLanguage': video_data.get('predictedLanguage', []),
                'duration': video_data.get('duration', 1000)
            } for video_id, video_data in all_data_instances.items()
        ])

        # Add the column 'languages' to the df
        textual_features_df['languages'] = textual_features_df.apply(get_languages, axis=1)

        # Fiter out undesired documents
        textual_features_df = textual_features_df[textual_features_df['duration'] <= 60]

        # Ensure directory exists and write to local file
        os.makedirs('data', exist_ok=True)
        textual_features_df.to_json(data_path, orient='records', lines=True)
        print("Data queried and written to local file.")

    return textual_features_df


def process_and_concatenate_textual_features(df, file_path=os.path.join(TOPIC_MODELING_DATA_DIR, 'textual_features.json'), force_overwrite=False):
    """
    Process and concatenate textual features from a DataFrame, then save the updated DataFrame as a JSON file.

    This function processes Wikipedia tags, combines title, description, tags, and processed Wikipedia tags into
    a single text column for each row in the DataFrame. It then cleans and preprocesses this combined text using
    a custom clean_text function.

    Parameters:
    - textual_features_df: DataFrame containing the textual features to be processed.
    - file_path: The path where the processed DataFrame will be saved as a JSON file.

    Returns:
    - The updated DataFrame with an additional 'processed_text' column.
    """
    # Check if the DataFrame already has a 'processed_text' column
    if 'processed_text' in df.columns and not force_overwrite:
        print("Text data has already been processed.")
        return df

    print("Processing text data...")
    # Vectorize the processing of Wikipedia tags using the 'process_wikipedia_tags' function
    df['processed_wikipedia_tags'] = df['wikipedia_tags'].apply(
        text_processing.process_wikipedia_tags)

    # Concatenate title, description, tags (joined by spaces), and processed Wikipedia tags into a single string
    df['combined_text'] = (df['title'].fillna('') + ' ' +
                           df['description'].fillna('') + ' ' +
                           df['tags'].apply(' '.join) + ' ' +
                           df['processed_wikipedia_tags'].fillna(''))

    # Apply the clean_text function to clean and preprocess the combined text
    # Example adjustment if parallelize_dataframe_processing returns a DataFrame
    df = text_processing.parallelize_dataframe_processing(df, text_processing.process_chunk, n_cores=8)

    # Create the directory if it doesn't exist and save the DataFrame as a JSON file
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    df.to_json(file_path, orient='records', lines=True)
    print(f"DataFrame with processed text saved to {file_path}")

    return df


def create_dictionary_and_corpus(df, min_token_frequency=5, no_above=0.05, keep_n=None, min_tokens_per_document=5):
    """
    Creates a dictionary and corpus from a DataFrame for topic modeling, and calculates the document frequency ratios
    for both remaining and filtered-out tokens.

    This function ensures that tokens are correctly accounted for after filtering extremes from the dictionary,
    providing accurate document frequency ratios for both remaining and filtered-out tokens.

    Parameters:
    - df: DataFrame with a 'processed_text' column containing preprocessed text documents.
    - min_token_frequency: Minimum frequency of tokens to be kept.
    - no_above: Maximum document frequency proportion for tokens.
    - keep_n: Maximum number of tokens to keep based on frequency.
    - min_tokens_per_document: Minimum number of tokens required for a document to be included in the corpus.

    Returns:
    - corpus: List of lists of (int, int) tuples representing the corpus.
    - id2word: Dictionary mapping token IDs to tokens.
    - df_filtered: DataFrame filtered based on token count criteria.
    - remaining_tokens_ratio: Dict of remaining tokens and their document frequency ratios.
    - filtered_out_tokens_ratio: Dict of filtered out tokens and their document frequency ratios.
    """
   # Ensure the DataFrame contains a column for processed text
    if 'processed_text' not in df.columns:
        raise ValueError("DataFrame must contain a 'processed_text' column")

    # Tokenize the processed text to prepare for dictionary creation
    df['tokens'] = df['processed_text'].apply(lambda x: x.split())

    # Filter out tokens that have only one character
    df['tokens'] = df['tokens'].apply(lambda tokens: [token for token in tokens if len(token) > 1])

    # Record the total number of documents before filtering
    total_documents = len(df)

    # Create a dictionary from the tokens of all documents and filter it
    dictionary = corpora.Dictionary(df['tokens'])
    pre_filter_tokens = {
        dictionary[id]: freq for id, freq in dictionary.dfs.items()}
    dictionary.filter_extremes(
        no_below=min_token_frequency, no_above=no_above, keep_n=keep_n)

    # Convert the token ID to word mapping into a more accessible format
    # Accessing an item to "load" the dictionary
    temp = dictionary[0]
    id2word = dictionary.id2token

    # Filter documents based on the count of tokens that remain after filtering
    df['token_count'] = df['tokens'].apply(lambda tokens: len(
        [token for token in tokens if token in dictionary.token2id]))
    df['unique_tokens'] = df['tokens'].apply(lambda tokens: list(dict.fromkeys(tokens)))
    df['unique_token_count'] = df['unique_tokens'].apply(lambda tokens: len(
        [token for token in tokens if token in dictionary.token2id]))


    filtered_df = df[df['unique_token_count'] >= min_tokens_per_document].copy()

    # Convert the filtered documents into a bag-of-words corpus
    corpus = [dictionary.doc2bow(text) for text in filtered_df['tokens']]

    # Calculate the ratio of documents each token appears in after filtering
    remaining_tokens_ratio = {
        token: dictionary.dfs[dictionary.token2id[token]] / total_documents for token in dictionary.token2id}
    remaining_tokens_ratio = sorted(
        remaining_tokens_ratio.items(), key=lambda x: x[1], reverse=True)

    # Identify and calculate the document frequency ratio for tokens filtered out
    filtered_out_tokens_ratio = {token: freq / total_documents for token,
                                 freq in pre_filter_tokens.items() if token not in dictionary.token2id}
    filtered_out_tokens_ratio = sorted(
        filtered_out_tokens_ratio.items(), key=lambda x: x[1], reverse=True)

    return corpus, id2word, df, filtered_df, remaining_tokens_ratio, filtered_out_tokens_ratio


def perform_topic_modeling(df, corpus, id2word, num_topics):
    """
    Perform topic modeling on a corpus using the NMF algorithm. 
    Stores the topic distribution for each document in the DataFrame as a list of scores.

    Parameters:
    - df (pandas.DataFrame): DataFrame containing the documents and their preprocessed text.
    - corpus (list of list of (int, int)): The corpus to be modeled, as a bag-of-words.
    - id2word (gensim.corpora.Dictionary): The dictionary mapping of ids to words.
    - num_topics (int): The number of topics to be generated by the NMF model.

    Returns:
    - pandas.DataFrame: The original DataFrame with an additional column 'topic_distribution_nmf'
                        containing the topic distribution as a list of scores for each document.
    """

    # Initialize the NMF model
    model = gensim.models.nmf.Nmf(corpus=corpus,
                                  num_topics=num_topics,
                                  id2word=id2word,
                                  passes=10,
                                  random_state=42,
                                  w_max_iter=400,
                                  w_stop_condition=0.00001,
                                  h_max_iter=100,
                                  h_stop_condition=0.0001)

    # Generate topic distribution for the entire corpus
    topic_distributions = [model.get_document_topics(
        bow, minimum_probability=0) for bow in corpus]

    # Convert topic distributions to lists of scores
    topic_scores = []
    for distribution in topic_distributions:
        # Initialize a list of zeros for each topic
        scores = [0] * num_topics
        for topic_id, score in distribution:
            scores[topic_id] = score
        topic_scores.append(scores)

    # Assign the list of scores to the DataFrame
    df['topic_distribution'] = topic_scores
    df['most_relevant_topic'] = df['topic_distribution'].apply(
        lambda scores: scores.index(max(scores)))

    return model, df
