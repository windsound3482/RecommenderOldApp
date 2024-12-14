import sys
sys.path.append('/Users/pablojerezarnau/git/RS-backend/')

import numpy as np
from Modules import database_queries
from config.settings import num_topics_in_database
from Modules.database_queries import es
np.float_ = np.float64


def read_topic_preferences_of_user(user_id):
    """
    Retrieves the topic preferences for the given user ID.

    Parameters:
    - user_id (str): The unique identifier of the user.

    Returns:
    - list: The list of topic preferences if found, otherwise None.
    """
    # Call the get_user_by_id function from database_queries to fetch the user data
    user_data = database_queries.get_user_by_id(user_id)

    if user_data and 'topic_preferences' in user_data:
        # Extract and return the 'topic_preferences' field from the user data
        return user_data['topic_preferences']
    else:
        print(f"No topic preferences found for user ID {user_id}.")
        return None


def processed_topic_scores_from_topic_preferences(topic_preferences):
    """
    Process the top-10 topic scores by subtracting the 11th highest score and normalize them.

    :param topic_preferences: numpy array of original topic scores.
    :return: A dictionary of the top-10 topics with their adjusted and normalized scores, sorted by score.
    """
        
    sorted_indices = np.argsort(topic_preferences)[::-1]
    cutoff_value = topic_preferences[sorted_indices[10]] 
    
    # Create a dictionary with the top-10 topics and their adjusted scores
    processed_scores = {index.item() :topic_preferences[index] -cutoff_value  for index in sorted_indices[:10]}
    sum_scores = sum(processed_scores.values())
    
    # Normalize the scores and sort the dictionary by value (score)
    processed_scores = {index: score / sum_scores for index, score in processed_scores.items()}
    sorted_processed_scores = {k: v for k, v in sorted(processed_scores.items(), key=lambda item: item[1], reverse=True)}
    print(sorted_processed_scores)
    return sorted_processed_scores


def update_topic_preferences_from_processed_topic_scores(user_id):
    """
    Read processed_topic_scores from the index 'users', calculate the new
    topic_preferences and upload it to the database.
    """
    # Read new processed_topic_scores from index 'users'
    print(f'Reading processed_topic_scores')
    processed_topic_scores = None
    old_topic_preferences = None
    try:
        response = es.get(index="users", id=user_id)
        if response['found']:
            processed_topic_scores =response['_source'].get('processed_topic_scores', None)
            old_topic_preferences = response['_source'].get('topic_preferences', None)
        else:
            print(f"User {user_id} not found.")      
    except Exception as e:
        print(f"An error occurred while fetching 'processed_topic_scores' for user {user_id}: {e}")

    # Calculate new topic_preferencdes
    print(f'Calculating topic_preferences from processed_topic_scores')
    if not isinstance(processed_topic_scores, dict):
        raise ValueError("processed_scores must be a dictionary with topic indices as keys and scores as values.")
    print(old_topic_preferences)

    new_topic_preferences = np.array(old_topic_preferences)

    # Correctly handle the summing of values from a dictionary
    top_10_scores = np.array([old_topic_preferences[int(i)] for i in processed_topic_scores.keys()])
    cutoff_value= np.min(top_10_scores)
    scaling_factor = np.sum(top_10_scores) - cutoff_value

    # Scale and adjust the top-10 scores
    for index, score in processed_topic_scores.items():
        # Ensure the new score does not fall below the cutoff value
        new_score = score * scaling_factor + cutoff_value
        new_topic_preferences[int(index)] = new_score  # Ensure index is used as an integer
    
    new_topic_preferences /= np.sum(new_topic_preferences)
    # Normalize the new topic preferences to sum to 1
    print(processed_topic_scores)
    print(f'writing new topic_preferences')
    database_queries.write_topic_preferences(user_id=user_id,
                                             topic_preferences=new_topic_preferences)

    return