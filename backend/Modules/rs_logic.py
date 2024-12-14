import sys
sys.path.append('/Users/pablojerezarnau/git/RS-backend/')

from Modules import helper_functions,database_queries, topic_preferences_management, new_personalised_rs, topic_categories_management
from Modules.topic_preferences_management import database_queries
import random
from config.settings import num_topics_in_database
import config
import numpy as np

from Modules.database_queries import es

num_topics_in_database = 300
    

def seachTopic(topics,excluded_videos):
    query = {
        "query": {
            "bool": {
                "must": {
                    "terms": {
                        "most_relevant_topic": topics
                    }
                },
                "must_not": {
                    "ids": {
                        "values": excluded_videos # List of video IDs to exclude
                    }
                }
            }
        },
        "sort": [
            {
                "statistics.viewCount": {
                    "order": "desc"
                }
            }
        ],
        "size": len(topics),
        "_source": ["statistics.viewCount",'most_relevant_topic']  # Request to return only the viewCount field
    }
   

    # Execute the search request
    return es.search(index="videos_test", body=query)

def update_recommended_topics_to_user(user_id, new_topics):
    """
    Update the list of recommended topics for a user in the 'users' index.
    Replace it with new_topics.

    :param user_id: The unique identifier for the user.
    :param new_topics: A list of new topics to add.
    """
    
    # Define the script for the update query
    script = {
        "source": "ctx._source.recommended_topics_in_top_popular_rs = params.topics;",
        "lang": "painless",
        "params": {
            "topics": new_topics
        }
    }

    # Execute the update query
    response = es.update(index="users", id=user_id, body={"script": script})
    resp = es.indices.refresh(
            index="users",
    )
    if response['result'] == 'updated':
        # print(f"Top-popular RS: updated user {user_id} with the new topics recommended by the top-popular RS.")
        pass
    else:
        print(f"Failed to update user {user_id}. Response: {response}")

    return response['result']
    

def get_recommendations(user_id):
    # Get n_recs_per_model
    n_recs_per_model = {}
    response = es.get(index="users", id=user_id)

    # Check if the document was found
    if response['found']:
        # Extract the 'n_recs_per_model' field from the document
        n_recs_per_model = response['_source'].get('n_recs_per_model', {})

    # Get all the entries of the user in the ES index 'feedback'
    feedback_entries = database_queries.get_all_feedback_by_user_id(user_id)

    # Extract the ID of the videos
    watched_videos_ids = [feedback['videoId'] for feedback in feedback_entries]

    disliked_channels_video_ids = database_queries.read_disliked_video_ids(user_id=user_id)
    excluded_videos = list(set(watched_videos_ids).union(set(disliked_channels_video_ids)))

    recommended_topics = None
    # Define the query to search for the user by user_id
    try:
        # Fetch the document for the specified user_id
        response = es.get(index="users", id=user_id)
        
        # Extract 'exploit_coeff' from the document
        recommended_topics = response['_source'].get('recommended_topics_in_top_popular_rs')
    except Exception as e:
        print(f"An error occurred while fetching 'recommended_topics_in_top_popular_rs' for user {user_id}: {e}")

    if len(recommended_topics) >= (num_topics_in_database - len(config.settings.filtered_topics)):
        print(f"All topics have been recommended to user {user_id} once by the top-popular RS. Resetting the topics loop.")
        update_recommended_topics_to_user(user_id=user_id,new_topics=[])
        recommended_topics = []

    # Sample new topics
    all_topics = set(range(num_topics_in_database))
    
    # Convert the list of seen topics to a set for efficiency
    seen_topics = set(recommended_topics)

    # Read list of filtered topics
    filtered_topics = set(config.settings.filtered_topics)
    
    # Determine the set of topics that have not been recommended yet
    available_topics = all_topics - seen_topics - filtered_topics
    
    # Ensure we don't try to sample more topics than available
    n_recs = min(n_recs_per_model['unpersonalised'], len(available_topics))
    
    # Sample `k` topics at random from the available topics, converting the set to a list
    new_topics = random.sample(list(available_topics), n_recs)
    print(f'Topics recommended by top-popular RS: ')
    # Get recommendations
    recommendations = []
    print(len(new_topics))

    # Execute the search request
    response = seachTopic(new_topics,excluded_videos)
    # Check if any documents were found
        # Extract the video ID
    for video in response['hits']['hits']:
        video_id = video['_id']
    # Extract the viewCount
        view_count = video['_source']['statistics']['viewCount']
        #Generate explanation for the recommendation
        formatted_view_count = helper_functions.format_number(num=view_count)
        explanation = f'Recommended to you because it was popular among other users ({formatted_view_count} views).'
        recommendations.append({"videoId": video_id, "explanation": explanation, "model": "top-popular"})
        # Return both video ID and viewCount
    # Update the user's profile with new recommended topics (if any new recommendations were made)
    if new_topics:
        response = update_recommended_topics_to_user(user_id=user_id,new_topics=new_topics)
    
    exploit_coeff = None
    topic_categories=None
    processed_topic_scores=None
    n_recs_exploit=None
    n_recs_explore=None
    try:
        # Get the document for the specified user_id
        response = es.get(index="users", id=user_id)
        if response.get('found', False):
            # 1. Get exploit_coeff
            exploit_coeff = response['_source'].get('exploit_coeff')
            # 3. Read topic_categories
            topic_categories = response['_source'].get('topic_categories', None)
            # 4. Read processed_topic_scores
            processed_topic_scores=response['_source'].get('processed_topic_scores', None)
        # 2. Get number of exploitative and explorative recommencations
    except Exception as e:
        print(f"An error occurred while fetching topic categories for user {user_id}: {e}")
    n_recs_exploit = round(n_recs_per_model['personalised'] * exploit_coeff)
    # Calculate number of recommendations for explore
    n_recs_explore = n_recs_per_model['personalised'] - n_recs_exploit
    

    # 5. Sample topics
    explorative_topics = new_personalised_rs.sample_explorative_topics(n_recs_explore=n_recs_explore,
                                                   topic_categories=topic_categories)
    
    if n_recs_exploit == 0:
        return[]
    
    # Get the "most_liked" topics
    most_liked_topics = topic_categories.get('most_liked', [])
    
    # Extract the scores for most_liked topics only
    scores = [processed_topic_scores.get(str(topic), 0) for topic in most_liked_topics]

    # print(f'Debugging - process_topic_scores: {processed_topic_scores}')
    
    # Sample topics based on the scores (probabilities)
    sampled_topic_indices = np.random.choice(
        most_liked_topics,
        size=n_recs_exploit,
        replace=True,  # Allow sampling the same topic more than once
        p=scores  # Use the scores as probabilities
    )

    exploitative_topics = sampled_topic_indices.tolist()
   
    response = seachTopic(exploitative_topics,  excluded_videos)
  
    for video in response['hits']['hits']:
        video_id = video['_id']
        explanation = new_personalised_rs.generate_explanation(topic=video['_source']['most_relevant_topic'], exploit_rec=True, exploit_coeff=exploit_coeff)
        # Append recommendation to the list
        recommendations.append({"videoId": video_id, "explanation": explanation, "model": "new personalised exploitation/exploration"})
        # Return both video ID and viewCount
   
    response = seachTopic(explorative_topics,  excluded_videos)
  
    for video in response['hits']['hits']:
        video_id = video['_id']
        explanation = new_personalised_rs.generate_explanation(topic=video['_source']['most_relevant_topic'], exploit_rec=False, exploit_coeff=exploit_coeff)
        # Append recommendation to the list
        recommendations.append({"videoId": video_id, "explanation": explanation, "model": "new personalised exploitation/exploration"})
    

    # Shuffle list of recommendations
    recommendations = random.sample(recommendations, len(recommendations))

    return recommendations


def register_user_parameters(user_id, liked_topic_ids):
    """
    Calculates user-specific values and updates the Elasticsearch index.
    
    :param user_id: The ID of the user to register.
    :return: A message indicating the outcome of the operation.
    """
    # Calculate topic_ratings
    num_topics = 300
    all_topic_ids = set(range(num_topics))
    liked_topic_ids_set = set(liked_topic_ids)
    unrated_topic_ids = list(all_topic_ids - liked_topic_ids_set)

    # Initialise data structures
    topic_preferences = [0] * num_topics_in_database

    # Calculate score for liked topics so that their sum is 1
    liked_topics_score = 0.99 / len(liked_topic_ids)
    defaultSetting={1:0.001,2:0.001,3:0.001,4:0.001,5:0.001,6:0.001,7:0.001,8:0.001,9:0.001,10:0.001}
    for topic_id,topic_value in defaultSetting.items():
        topic_preferences[topic_id] = topic_value
    # Assign scores to liked topics
    for topic_id in liked_topic_ids:
        topic_preferences[topic_id] = liked_topics_score
    topic_categories = {
        "most_liked": liked_topic_ids,
        "unrated": [],
        "rated_but_not_most_liked": []  # Assuming this needs to be populated elsewhere
    }

    # Populate the "unrated" list with all topic indices except those specified as liked
    topic_categories["unrated"] = [
        topic_index for topic_index in range(num_topics_in_database)
        if topic_index not in (liked_topic_ids + config.settings.filtered_topics)
    ]

    sorted_indices = np.argsort(topic_preferences)[::-1]
    cutoff_value = topic_preferences[sorted_indices[10]] 
    
    # Create a dictionary with the top-10 topics and their adjusted scores
    processed_scores = {index.item() :topic_preferences[index] -cutoff_value  for index in sorted_indices[:10]}
    sum_scores = sum(processed_scores.values())
    
    # Normalize the scores and sort the dictionary by value (score)
    # Remove all 0-values from processed_topic_scores
    processed_scores = {index: score / sum_scores for index, score in processed_scores.items()}
    processed_topic_scores = {k: v for k, v in sorted(processed_scores.items(), key=lambda item: item[1], reverse=True) if v!=0}
    print(processed_topic_scores)

    # Calculate the values for each field based on business logic
    user_data = {
        "n_recs_per_model": {
            "personalised": 5,
            "unpersonalised": 5,
        },
        "exploit_coeff": 0.5,
        "topic_preferences": topic_preferences,
        "topic_categories": topic_categories,
        "recommended_topics_in_top_popular_rs": [],
        "processed_topic_scores": processed_topic_scores,
        "topic_ratings": {
            'liked': liked_topic_ids,
            'disliked': [],
            'unrated': unrated_topic_ids
        },
        "disliked_creators_video_ids": [],
        "disliked_creators":[]
    }
    
    # Delegate the actual update to the database_queries module
    try:
        # Example Elasticsearch update operation
       
        response = es.update(index="users", id=user_id, body={"doc": user_data}, doc_as_upsert=True)
        resp = es.indices.refresh(
        index="users",
        )
        if response['result'] in ['updated', 'created']:
            return f'User {user_id} registered and preferences updated successfully.'
        else:
            return f'Failed to update preferences for user {user_id}.'
    except Exception as e:
        return f'Error updating user {user_id}: {str(e)}'
   
    