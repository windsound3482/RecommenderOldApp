import json

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import os

import numpy as np
import pandas as pd
from wordcloud import WordCloud

from Modules import database_queries
from config.settings import TOPIC_MODELING_RUNS_DIR


def write_insights_to_file(filtered_df, df, remaining_tokens_ratio, filtered_out_tokens_ratio, topic_modeling_params, file_path):
    """
    Writes insights about the document filtering process, token frequency ratios, topic modeling parameters, 
    and token filtering statistics to a JSON file.

    This function documents the number and ratio of documents filtered out during preprocessing,
    lists the tokens that were filtered out and those that remained, along with their document frequency ratios,
    includes the parameters used for topic modeling, and provides statistics on the total and filtered out tokens.

    Parameters:
    - filtered_df: DataFrame after filtering based on token count criteria.
    - df: Original DataFrame before filtering.
    - remaining_tokens_ratio: List of tuples containing remaining tokens and their document frequency ratios.
    - filtered_out_tokens_ratio: List of tuples containing filtered out tokens and their document frequency ratios.
    - topic_modeling_params: Dictionary with parameters used for topic modeling.
    - file_path: Path to the output JSON file where insights will be saved.

    The output JSON file is structured with keys for document statistics, tokens (including filtering parameters and ratios), 
    topic modeling parameters, and token statistics.
    """

    # Prepare the token statistics for inclusion in the JSON file
    n_remaining_tokens = len(remaining_tokens_ratio)
    n_filtered_out_tokens = len(filtered_out_tokens_ratio)
    total_token_count = n_remaining_tokens + n_filtered_out_tokens
    filtered_out_token_ratio = n_remaining_tokens / \
        total_token_count if total_token_count else 0

    insights = {
        'document_statistics': {
            'total_documents': len(df),
            'filtered_out_documents': len(df)-len(filtered_df),
            'remaining_documents': len(filtered_df),
            'ratio_of_remaining_documents': round(len(filtered_df) / len(df), 4),
            'min_token_count': topic_modeling_params['min_tokens_per_document']
        },
        'tokens': {
            'total_tokens': total_token_count,
            'filtered_out_tokens': n_filtered_out_tokens,
            'remaining_tokens': n_remaining_tokens,
            'ratio_of_remaining_tokens': round(filtered_out_token_ratio, 4),
            'filtering_parameters': {
                'no_above': topic_modeling_params['no_above'],
                'no_below': topic_modeling_params['min_token_frequency'],
                # This will insert the keep_n value if present, otherwise it will be None
                'keep_n': topic_modeling_params.get('keep_n')
            },
            'remaining': [{
                'token': token,
                'frequency_ratio': ratio
            } for token, ratio in remaining_tokens_ratio],
            'filtered_out': [{
                'token': token,
                'frequency_ratio': ratio
            } for token, ratio in filtered_out_tokens_ratio]
        }
    }

    # Write the structured insights into a JSON file with indentation for readability
    with open(file_path, 'w') as f:
        # Serialize the insights dictionary into the specified file with indentation for readability
        json.dump(insights, f, indent=4)


def save_topics_with_document_counts_and_plot(topic_modeling_df, model, run_dir, tm_params):
    """
    Saves topic information along with the count of documents for which each topic is the most relevant,
    sorted by descending document count, and creates a sorted bar plot of the number of documents per topic.

    Parameters:
    - topic_modeling_df (pd.DataFrame): DataFrame containing the topic modeling results, including 'most_relevant_topic'.
    - model (gensim.models): The trained topic model.
    - run_dir (str): Directory path to save the topics JSON file and the plot.
    - tm_params (dict): Topic modeling parameters, including 'num_topics'.
    """
    # Calculate the number of documents per topic
    documents_per_topic = topic_modeling_df['most_relevant_topic'].value_counts().sort_index()

    # Generate the topic information
    topics = [
        {
            "topic_number": int(topic_id),
            "document_count": int(documents_per_topic.get(topic_id, 0)),
            "tokens": [{word: value for word, value in model.show_topic(topic_id, 10)}]
        }
        for topic_id in range(tm_params['num_topics'])
    ]

    # Sort topics by descending number of documents
    topics_sorted = sorted(topics, key=lambda x: x['document_count'], reverse=True)

    # Save the sorted topic information to a JSON file
    topics_path = os.path.join(run_dir, "topics.json")
    with open(topics_path, 'w') as f:
        json.dump(topics_sorted, f, indent=4)

    print(f"Saved sorted topic information with document counts to {topics_path}.")

    # Sort the document counts in descending order
    documents_per_topic_sorted = documents_per_topic.sort_values(ascending=False)

    # Create and save a bar plot of the number of documents per topic
    plt.figure(figsize=(10, 6))
    plt.bar(range(len(documents_per_topic_sorted)), documents_per_topic_sorted.values, color='skyblue')
    plt.xlabel('Topics (sorted by document count)')
    plt.ylabel('Number of Documents')
    plt.title('Number of Documents per Topic (Descending Order)')
    plt.xticks([])  # Remove x-axis labels

    plot_path = os.path.join(run_dir, "documents_per_topic_sorted.png")
    plt.savefig(plot_path)
    print(f"Saved sorted bar plot of the number of documents per topic to {plot_path}.")
    plt.close()


def generate_and_save_wordclouds_from_df(topic_modeling_df, model, run_dir, num_topics, cols=10):
    """
    Generates and saves high-quality word clouds for each topic based on the topic modeling DataFrame,
    and creates a high-resolution grid image of all word clouds, with each row displaying 10 word clouds.

    Parameters:
    - topic_modeling_df (pd.DataFrame): DataFrame containing the topic modeling results.
    - model (gensim.models): The trained topic model used for topic modeling.
    - run_dir (str): The directory path for the run.
    - num_topics (int): The total number of topics.
    - cols (int): Number of columns in the grid, set to 10.
    """
    wordclouds_dir = os.path.join(run_dir, "wordclouds")
    os.makedirs(wordclouds_dir, exist_ok=True)

    rows = num_topics // cols + (num_topics % cols > 0)

    # Increase figure size and DPI for high-quality output with more word clouds per row
    fig_width = cols * 3  # e.g., 30 inches width for 10 columns
    fig_height = rows * 3  # height for each row, adjust as needed
    fig, axes = plt.subplots(rows, cols, figsize=(fig_width, fig_height), dpi=100)
    axes = axes.flatten() if isinstance(axes, np.ndarray) else [axes]

    for topic_id in range(num_topics):
        word_freq = dict(model.show_topic(topic_id, 200))  # Get top N words for the topic
        wordcloud = WordCloud(background_color='white', max_words=50, collocations=False, width=400, height=300).generate_from_frequencies(word_freq)

        # Save individual word cloud to file with high resolution
        wc_path = os.path.join(wordclouds_dir, f"topic_{topic_id}_wordcloud.png")
        wordcloud.to_file(wc_path)

        # Display the word cloud in the grid
        ax = axes[topic_id] if topic_id < len(axes) else plt.gca()
        ax.imshow(wordcloud, interpolation='bilinear')
        ax.axis('off')
        ax.set_title(f"Topic {topic_id}", size=14, pad=20)  # Adjust title size and padding

    # Remove empty subplots if any
    for j in range(num_topics, len(axes)):
        axes[j].set_visible(False)

    plt.tight_layout(pad=3.0)  # Adjust layout padding
    # Save the grid of word clouds to file with high resolution
    grid_path = os.path.join(run_dir, "wordclouds_grid.png")
    plt.savefig(grid_path, dpi=300)  # Save with high DPI for better quality
    plt.close()

    print(f"Individual word clouds saved to {wordclouds_dir}.")
    print(f"High-quality grid of all word clouds saved to {grid_path}.")


def save_topic_videos_overview(topic_modeling_df, run_dir, k=10):
    """
    Saves an overview of top-k videos for each topic based on their relevance scores to a JSON file.
    ...
    """
    # First, ensure there's a column for the topic scores. This step calculates the score for the most relevant topic.
    # This assumes 'topic_distribution' is a list of scores, with one score per topic, and matches the order of topics.
    topic_modeling_df['topic_score'] = topic_modeling_df.apply(
        lambda row: row['topic_distribution'][row['most_relevant_topic']], axis=1)

    # Initialize a dictionary to hold the video lists for each topic
    topic_videos = {}

    # Iterate through each unique topic ID in the DataFrame
    for topic_id in sorted(topic_modeling_df['most_relevant_topic'].unique()):
        topic_id_serializable = int(topic_id)  # Ensure topic_id is serializable

        # Select the top-k videos for the current topic based on the topic score, sort them in descending order
        top_videos = topic_modeling_df[topic_modeling_df['most_relevant_topic'] == topic_id] \
            .sort_values('topic_score', ascending=False) \
            .head(k)[['id', 'topic_score', 'title', 'link']] \
            .to_dict(orient='records')

        topic_videos[topic_id_serializable] = top_videos

    filepath = os.path.join(run_dir, 'topic_videos_overview.json')
    with open(filepath, 'w') as f:
        json.dump(topic_videos, f, indent=4, ensure_ascii=False)

    print(f"Saved topic videos overview to {filepath}")


def save_topic_distributions(topic_modeling_df, run_dir):
    """
    Saves the topic distributions of videos to a JSON file during the topic modeling process. Additionally,
    enriches the DataFrame with a 'most_relevant_topics' column that includes the top three topics for each video
    along with their scores.

    Parameters:
    - topic_modeling_df (DataFrame): A DataFrame containing the topic distributions and other video metadata.
    - run_dir (str): The directory path where the output JSON file will be saved.

    The function assumes that 'topic_distribution' is a list of floats representing the distribution of topics for each video,
    'most_relevant_topic' is an integer representing the most relevant topic index for each video,
    'title' is the video title, and 'link' is the URL to the video.
    """

    # Add a column 'most_relevant_topics' to the DataFrame
    def get_most_relevant_topics(distribution):
        # Sort the topics by score in descending order and take the top three
        top_topics = sorted(enumerate(distribution), key=lambda x: x[1], reverse=True)[:3]
        # Return a dictionary of the top three topics and their scores
        return {topic_id: score for topic_id, score in top_topics}

    # Apply the function to the 'topic_distribution' column
    topic_modeling_df['most_relevant_topics'] = topic_modeling_df['topic_distribution'].apply(get_most_relevant_topics)

    # Define the path where the topic distributions will be saved
    topic_distributions_path = os.path.join(run_dir, "topic_distributions.json")

    # Select the relevant columns for saving
    columns_to_save = ['id', 'topic_distribution', 'most_relevant_topic', 'title', 'link', 'most_relevant_topics']
    # Save the DataFrame to a JSON file with one line per record
    topic_modeling_df[columns_to_save].to_json(topic_distributions_path, orient='records', lines=True)


def update_topic_distributions_in_es(run_id):
    """
    For the given topic modeling run_id, find the topic distributions in data/runs/topic_modeling/<run_id>/topic_distributions.json.
    It is a JSON of a DataFrame with 'id' and 'topic_distribution'. This function calls the 
    database_queries.write_topic_distributions(topic_modelling_df) to write the topic distributions 
    to the ElasticSearch database and then updates the variable tm_run_in_database in config/settings.py
    only if the database update is successful.
    """
    # Construct the file paths
    BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    settings_path = os.path.join(BASE_DIR, 'config/settings.py')
    topic_distributions_path = os.path.join(TOPIC_MODELING_RUNS_DIR, run_id, 'topic_distributions.json')

    # Read the newline-delimited JSON file line by line and convert to a DataFrame
    with open(topic_distributions_path, 'r') as file:
        lines = file.readlines()
    # Parse each line as a JSON object and collect them into a list
    data = [json.loads(line) for line in lines]
    topic_distributions_df = pd.DataFrame(data)

    try:
        # Attempt to write the topic distributions to the database
        database_queries.write_topic_distributions(topic_distributions_df)

        # If successful, update the settings.py file
        settings_updated = False
        with open(settings_path, 'r') as file:
            lines = file.readlines()

        with open(settings_path, 'w') as file:
            for line in lines:
                if 'tm_run_in_database' in line:
                    # Assuming the settings.py has a line like: tm_run_in_database = '<old_run_id>'
                    line = f"tm_run_in_database = '{run_id}'\n"
                    settings_updated = True
                file.write(line)

        return settings_updated

    except Exception as e:
        # Handle any exceptions that occurred during the database update
        print(f"An error occurred while updating the topic distributions: {e}")
        return False
        