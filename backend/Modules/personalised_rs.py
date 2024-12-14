import sys
sys.path.append('/Users/pablojerezarnau/git/RS-backend/')

from Modules import topic_preferences_management, personalised_rs_database_queries, database_queries
from Modules import rs_logic
from Modules.database_queries import es

def update_topic_ratings(user_id, n_liked_topics=10):
    """
    Get a user's topic preference vector from the index 'users', calculate the
    'liked', 'disliked', and 'unrated' topics and update the dict 'topic_ratings'
    in the index 'users'.
    """
    # Get the topic preference vector
    topic_preferences = topic_preferences_management.read_topic_preferences_of_user(user_id=user_id)
    
    # Sort the topic preferences by their scores, keeping track of the topic indices
    sorted_preferences_indices = sorted(range(len(topic_preferences)), key=lambda i: topic_preferences[i], reverse=True)
    
    # The top-20 topics are 'liked'
    liked_topics = sorted_preferences_indices[:n_liked_topics-1]
    
    # Assuming all other topics are 'unrated'
    unrated_topics = sorted_preferences_indices[n_liked_topics-1:]
    
    # Define the dict topic_ratings
    topic_ratings = {
        'liked': liked_topics,
        'disliked': [],
        'unrated': unrated_topics
    }

    # Update the topic ratings in the Elasticsearch index 'users'
    update_status = personalised_rs_database_queries.upload_topic_ratings(user_id, topic_ratings)

    return update_status


def coeff_to_percentile_window(coeff):
    """
    Maps a given coefficient from 0.6 to 1 to a corresponding percentile window using a dictionary.
    Explicitly handles the case where coefficient is 0.5 as not allowed.
    
    :param coeff: A float value from 0.6 to 1.
    :return: A tuple representing the percentile window (lower_bound, upper_bound).
    """
    # Check for the disallowed value.
    if coeff == 0.5:
        raise ValueError("Coefficient of 0.5 is not allowed.")
    
    if not 0.6 <= coeff <= 1:
        raise ValueError("Coefficient must be between 0.6 and 1.")

    # Define mappings from coefficient ranges to percentile windows.
    windows = {
        0.6: (80, 100),
        0.7: (60, 80),
        0.8: (40, 60),
        0.9: (20, 40),
        1.0: (0, 20),
    }

    # Find the closest matching key in the dictionary that does not exceed the coeff.
    key = max(k for k in windows.keys() if k <= coeff)
    
    return windows[key]


def pretty_print_query_results(results, n_recs):
    """
    Pretty prints the results of a query, focusing on specific fields.

    :param results: The search results returned by Elasticsearch.
    """
    if results['hits']['total']['value'] > 0:
        print(f"Found {results['hits']['total']['value']} results:")
        for hit in results['hits']['hits'][:n_recs]:
            id = hit['_source'].get('id', 'No ID')
            title = hit['_source'].get('title', 'No Title')
            link = hit['_source'].get('link', 'No Link')
            most_relevant_topics_dict = hit['_source'].get('most_relevant_topics_dict', {})

            print(f"\nID: {id}")
            print(f"Title: {title}")
            print(f"Link: {link}")
            print("Most Relevant Topics:")

            # Iterate over the sorted keys to maintain order
            for key in ["1", "2"]:
                topic = most_relevant_topics_dict[key]
                topic_index = topic.get('topic_index', 'N/A')
                topic_score = topic.get('topic_score', 'N/A')
                percentile = topic.get('percentile', 'N/A')
                if percentile is not 'N/A': percentile = round(percentile)
                print(f" - Topic Index: {topic_index}, Score: {round(topic_score, 2)}, Percentile: {percentile}")
    else:
        print("No results found.")


def get_recommended_videos(query_results, n_recs):
    """
    Sample n_recs videos from the query results. For each video, get the video id, the most
    relevant topic's index and the second most relevant topic's index. Store them as a list of dicts.

    :param query_results: The search results returned by Elasticsearch.
    :param n_recs: Number of recommendations to return.
    :return: A list of dictionaries with video id, and the indices of the most and second most relevant topics.
    """
    recommendations = []

    # Check if there are enough results; otherwise, take as many as possible
    hits = query_results['hits']['hits']
    n_samples = min(n_recs, len(hits))

    for hit in hits[:n_samples]:
        video_id = hit['_id']  # Assuming the video ID is stored in the document ID
        most_relevant_topics = hit['_source'].get('most_relevant_topics_dict', {})

        # Sort the keys to ensure the order is by relevance
        sorted_keys = sorted(most_relevant_topics.keys(), key=lambda x: int(x))
        most_relevant_topic_index = most_relevant_topics[sorted_keys[0]].get('topic_index') if sorted_keys else None
        second_most_relevant_topic_index = most_relevant_topics[sorted_keys[1]].get('topic_index') if len(sorted_keys) > 1 else None

        recommendation = {
            'video_id': video_id,
            'most_relevant_topic_index': most_relevant_topic_index,
            'second_most_relevant_topic_index': second_most_relevant_topic_index
        }
        recommendations.append(recommendation)

    return recommendations


def generate_individual_explanation(exploit_coeff, video_info):
    """
    Take the dict video_info with the keys 'video_id', 'most_relevant_topic_index' and 'second_'most_relevant_topic_index'.
    Generate an explanatin as a string, whose text depends on the value of exploit_coeff (exploitative/explorative).
    """
    # Get topic descriptions
    most_relevant_topic_description = database_queries.get_topic_description(topic_index=video_info['most_relevant_topic_index'])
    second_most_relevant_topic_description = database_queries.get_topic_description(video_info['second_most_relevant_topic_index'])

    # Generate explanation
    if exploit_coeff > 0.5:
        explanation = f"""\
Recommended to you because \
you seem to like topic '{most_relevant_topic_description}' \
and because \
you have not come acros videos of topic '{second_most_relevant_topic_description}'."""
    else:
        explanation = f"""\
Recommended to you because \
you have not come acros videos of topic '{second_most_relevant_topic_description}' \
and because \
you seem to like topic '{most_relevant_topic_description}'."""

    return explanation


def get_recommendations(user_id, n_recs):
    """
    Get the recommendatins of the personalised RS. Get a list of n_recs recommendation dicts.
    """
    # Get list of watched videos
    watched_videos_ids = rs_logic.get_videos_rated_by_user(user_id=user_id)

    # Get recommendation parameters
    exploit_coeff = None
    try:
        # Fetch the document for the specified user_id
        response = es.get(index="users", id=user_id)
        
        # Extract 'exploit_coeff' from the document
        exploit_coeff = response['_source'].get('exploit_coeff')
        topic_ratings = response['_source'].get('topic_ratings')

    except Exception as e:
        print(f"An error occurred while fetching 'exploit_coeff' for user {user_id}: {e}")
    percentile_bounds=None
    most_relevant_topics=None
    second_most_relevant_topics=None
    # Define query parameters
    # Exploitative recs
    if exploit_coeff > 0.5:
        percentile_bounds = coeff_to_percentile_window(exploit_coeff)
        most_relevant_topics = topic_ratings['liked']
        second_most_relevant_topics = topic_ratings['unrated']
    # Explorative recs
    else:
        percentile_bounds = coeff_to_percentile_window(1-exploit_coeff)
        most_relevant_topics = topic_ratings['unrated']
        second_most_relevant_topics = topic_ratings['liked']

    

    # Perform query
    query_results = personalised_rs_database_queries.execute_query(percentile_window=percentile_bounds,
                                                                   most_relevant_topics=most_relevant_topics,
                                                                   second_most_relevant_topics=second_most_relevant_topics,
                                                                   exclude_video_ids=watched_videos_ids)
    
    # Optional: pretty print info of recommended videos
    pretty_print_query_results(results=query_results, n_recs=n_recs)

    # Process query results
    recommended_videos = get_recommended_videos(query_results=query_results, n_recs=n_recs)

    # Generate explanations
    explanations = [generate_individual_explanation(exploit_coeff=exploit_coeff, video_info=video_info) for video_info in recommended_videos]

    # Create recommendation list
    recommendations = []
    for i in range(n_recs):

        video_id = recommended_videos[i]['video_id']
        explanation = explanations[i]
        recommendation = {"videoId": video_id, "explanation": explanation, "model": "personalised exploitation/exploration"}
        recommendations.append(recommendation)

    return recommendations