import sys

import config.settings
sys.path.append('/Users/pablojerezarnau/git/RS-backend/')

import Modules
from Modules import topic_preferences_management, database_queries
from Modules.database_queries import es
from config.settings import num_topics_in_database
import config


def get_relevant_topics_from_videos(video_ids):
    """
    Query the index 'topic_distributions' and get the 3 most relevant
    topics of each video. Return list of topic indices without duplicates.
    Use the field most_relevant_topics_dict.{1, 2, 3}.topic_index.
    
    :param video_ids: A list of video IDs to query.
    :return: A list of unique topic indices.
    """
    # Build the query
    query = {
        "query": {
            "bool": {
                "filter": {
                    "terms": {
                        "_id": video_ids
                    }
                }
            }
        },
        "_source": ["most_relevant_topics_dict.1.topic_index", 
                    "most_relevant_topics_dict.2.topic_index",
                    "most_relevant_topics_dict.3.topic_index"]
    }

    # Execute the search query
    response = es.search(index="topic_distributions", body=query, size=len(video_ids))
    
    # Extract topic indices
    topic_indices = set()  # Use a set to avoid duplicates
    for hit in response['hits']['hits']:
        most_relevant_topics_dict = hit['_source']['most_relevant_topics_dict']
        # Extract topic indices from the nested dictionaries
        for key in most_relevant_topics_dict:
            topic_index = most_relevant_topics_dict[key].get('topic_index')
            if topic_index is not None:
                topic_indices.add(topic_index)
                
    return list(topic_indices)


def get_rated_topics(user_id):
    """
    Get all the user's feedback entries from the index 'feedback'. Return the
    topics that have been rated in two possible manners: either getting an explicit
    topic rating, or being in the top-3 most relevant topics of a video that got an
    explicit video rating.
    
    :param user_id: The ID of the user whose rated topics are being queried.
    :return: A list of unique topic indices that have been rated.
    """
    # Fetch all feedback entries for the user
    feedback_entries = database_queries.get_all_feedback_by_user_id(user_id=user_id)
    
    # Get topics from videos with explicit ratings
    video_ids_with_explicit_rating = [entry['videoId'] for entry in feedback_entries if entry.get('rating', 0) != 0]
    
    # Next, fetch the most relevant topics for these videos
    topics_from_video_ratings= get_relevant_topics_from_videos(video_ids_with_explicit_rating)


    # Get topics with explicit topic ratings
    topics = set()  # Use a set to avoid duplicates

    for entry in feedback_entries:
        more_topics = entry.get('more', [])
        less_topics = entry.get('less', [])
        
        # Convert topic IDs from strings to integers if necessary and add to the set
        topics.update([int(topic_id) for topic_id in more_topics])
        topics.update([int(topic_id) for topic_id in less_topics])
    
    topics_from_topic_ratings = list(topics)

    # Combine both lists of topics into a set to remove duplicates, then convert back to a list
    rated_topics = set(topics_from_video_ratings + topics_from_topic_ratings)

    return list(rated_topics)


def calculate_topic_categories(user_id):
    """
    Retrieves and organizes the user's topic preferences into categories.

    This function categorizes topics into 'most_liked', 'unrated', and 'rated_but_not_most_liked'
    based on the user's interactions and feedback. It aims to provide a structured view of the user's
    preferences for further analysis or recommendation adjustments.

    Parameters:
    - user_id (str): The unique identifier of the user for whom topic categories are retrieved.

    Returns:
    - dict: A dictionary containing lists of topic indices for each category:
        - 'most_liked': Topics with the highest preference scores.
        - 'rated_but_not_most_liked': Topics that have been explicitly rated but are not among the most liked.
        - 'unrated': Topics that have not been rated or interacted with.
    """
    # Calculate most liked topics based on user feedback
    topic_preferences = topic_preferences_management.read_topic_preferences_of_user(user_id=user_id)
  
    # take the first 10, then extract only the indices.
    most_liked_topics = [index for index, score in sorted(enumerate(topic_preferences), key=lambda x: x[1], reverse=True)[:10]]
    most_liked_topics = list(set(most_liked_topics) - set(config.settings.filtered_topics))

    # Subtract the set of most liked topics from the set of rated topics
    rated_but_not_most_liked = list(set(get_rated_topics(user_id=user_id)) - set(most_liked_topics)  - set(config.settings.filtered_topics))

    # Determine unrated topics by subtracting rated sets from the full set
    unrated_topics = sorted(list(set(range(num_topics_in_database)) - set(rated_but_not_most_liked) - set(most_liked_topics)  - set(config.settings.filtered_topics)))
    
    # Organize topics into categories
    topic_categories = {
        "most_liked": most_liked_topics,
        "unrated": unrated_topics,
        "rated_but_not_most_liked": rated_but_not_most_liked
    }

    # print(f'topic_categories: {topic_categories}')

    return topic_categories
