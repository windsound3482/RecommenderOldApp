import sys
sys.path.append('/Users/pablojerezarnau/git/RS-backend/')

from Modules import helper_functions
from elasticsearch import Elasticsearch, helpers
from elasticsearch.exceptions import TransportError, NotFoundError
from elasticsearch.helpers import bulk, scan
import time
from langdetect import detect, LangDetectException
import json
import os


# Define database url and credentials
url = "http://localhost:9200/"
username = "elastic"
password = "gamUBg0KZZ0w5i6tikd0"

# Setup the connection to Elasticsearch
es = Elasticsearch(
    url,
    basic_auth=(username, password)
)



def parse_hit(hit):
    video_id = hit['_id']
    source = hit['_source']
    defaultLanguage = ''
    defaultAudioLanguage = ''
    predicted_language = ''
    title = ''
    description = ''
    tags = []
    # Check if 'snippet' exists in the source, if not set fields to default values
    if 'snippet' in source:
        snippet = source['snippet']
        
        # Extract or set default values for language fields
        defaultLanguage = snippet.get('defaultLanguage', '')
        defaultAudioLanguage = snippet.get('defaultAudioLanguage', '')
        
        # Predict language based on title and description
        predicted_language = ''
        try:
            text = snippet['title'] + " " + snippet['description']
            predicted_language = detect(text)
        except LangDetectException:
            print('langerror')
        # Extract additional data from snippet
        title = snippet['title']
        description = snippet['description']
        tags = snippet.get('tags', [])

    video_data = {
        'id': video_id,
        'title': title,
        'description': description,
        'statistics': source.get('statistics', {}),
        'link': f'https://www.youtube.com/watch?v={video_id}',
        'tags': tags,
        'topicCategories': source.get('topicDetails', {}).get('topicCategories', []),
        'defaultLanguage': defaultLanguage,  
        'defaultAudioLanguage': defaultAudioLanguage,
        'topicDistribution': source.get('topic_distribution', []),
        'predictedLanguage': predicted_language,
    }
    
    # format duration field if available
    if source.get('contentDetails', {}).get('duration', ''):
        video_data['duration'] = helper_functions.iso8601_duration_to_seconds(source.get('contentDetails', {}).get('duration', ''))

    return video_data




def get_entire_database():
    """
    Retrieves the entire contents of the video database from Elasticsearch.

    Returns:
    - dict: A dictionary with video IDs as keys and video details as values.
    """
    # Initialize the empty dictionary to hold all video details
    all_videos = {}

    # Initialize the scroll
    try:
        page = es.search(
            index='videos',
            scroll='10m',  # Length of time to keep the Scroll alive
            size=2000,  # Number of results to return per batch
            body={"query": {"match_all": {}}}
        )
    except TransportError as e:
        print(f"Error initiating scroll: {e}")
        return all_videos

    scroll_id = page['_scroll_id']
    scroll_size = page['hits']['total']['value']
    print(f"Scrolling {scroll_size} videos")

    try:
        while scroll_size > 0:
            # Scroll the batch of results
            page = es.scroll(scroll_id=scroll_id, scroll='10m')

            # Update the scroll ID for the next batch
            scroll_id = page['_scroll_id']

            # Get the number of results that we returned in the last scroll
            scroll_size = len(page['hits']['hits'])

            # Process the batch
            videos_dict = {}
            for hit in page['hits']['hits']:

                # parse hit
                video_data = parse_hit(hit=hit)

                # add video data to the list
                videos_dict[video_data['id']] = video_data
            all_videos.update(videos_dict)

            # Break after the last scroll
            if scroll_size == 0:
                break

    except TransportError as e:
        print(f"Error during scrolling: {e}")
    finally:
        # Always clear the scroll
        try:
            es.clear_scroll(scroll_id=scroll_id)
        except TransportError as e:
            print(f"Error during scrolling: {e}")

    return all_videos


def remove_topic_distributions_from_all_videos():
    """
    Removes the topic_distribution field from all video documents in Elasticsearch.
    """
    # Match all documents in the videos index
    body = {
        "query": {"match_all": {}},
        "script": {
            "source": "ctx._source.remove('topic_distribution')"
        }
    }
    try:
        es.update_by_query(index='videos', body=body,
                           scroll_size=200, request_timeout=5*60)
        resp = es.indices.refresh(
            index='videos',
        )

        print("Removed topic_distribution from all videos.")
    except Exception as e:
        print(f"Error removing topic_distribution from videos: {e}")


def write_topic_distributions(df, batch_size=200):
    """
    Writes or updates the topic_distribution field for multiple video documents in Elasticsearch using bulk operations,
    with operations grouped in batches to be mindful of database limitations.
    First removes the topic_distribution field from all documents to ensure only current distributions remain.

    Parameters:
    - df: DataFrame containing video IDs and their corresponding topic distributions.
    - batch_size: The maximum number of operations to include in a single batch.
    """
    # Remove existing topic_distribution from all documents first
    remove_topic_distributions_from_all_videos()

    actions = []

    # Function to process actions in bulk
    def process_bulk_actions(actions_batch):
        try:
            successes, _ = bulk(es, actions_batch)
            print(
                f"Successfully updated/added topic_distribution for {successes} videos.")
        except Exception as e:
            print(f"Error updating/adding topic_distribution for videos: {e}")

    for index, row in df.iterrows():
        video_id = row['id']
        topic_distribution = row['topic_distribution']

        # Ensure topic_distribution is a list of floats
        if not isinstance(topic_distribution, list):
            topic_distribution = [topic_distribution]
        topic_distribution = [float(value) for value in topic_distribution]

        action = {
            "_op_type": "update",
            "_index": "videos",
            "_id": video_id,
            "doc": {"topic_distribution": topic_distribution},
            "doc_as_upsert": True
        }

        actions.append(action)

        # If the current batch is full, process it
        if len(actions) >= batch_size:
            process_bulk_actions(actions)
            actions = []  # Reset actions for the next batch

    # Process any remaining actions that didn't fill the last batch
    if actions:
        process_bulk_actions(actions)


def upload_topic_distributions_to_database(run_id, TOPIC_MODELING_RUNS_DIR):
    """
    Reads the topics.json file from a specific run directory and uploads
    the data to the 'topic_distributions' index in Elasticsearch.
    
    :param run_id: The ID of the topic modeling run.
    :param TOPIC_MODELING_RUNS_DIR: The root directory containing run directories.
    """
    file_path = os.path.join(TOPIC_MODELING_RUNS_DIR, run_id, 'topic_distributions.json')
    
    # Remove existing documents from the index for this run
    query = {"query": {"match_all": {}}}
    es.delete_by_query(index="topic_distributions", body=query)
    
    # Prepare bulk upload data
    actions = []
    with open(file_path, 'r', encoding='utf-8') as file:
        for line in file:
            doc = json.loads(line)
            most_relevant_topics = [{"topic_index": k, "topic_score": v} for k, v in doc['most_relevant_topics'].items()]
            action = {
                "_index": "topic_distributions",
                "_id": doc['id'],
                "_source": {
                    "video_id": doc['id'],
                    "topic_distribution": doc['topic_distribution'],
                    "most_relevant_topics": most_relevant_topics,
                    "most_relevant_topic": doc['most_relevant_topic']
                }
            }
            actions.append(action)

    # Use the bulk API to perform the index operations
    if actions:
        helpers.bulk(es, actions)
        print(f"Uploaded {len(actions)} documents to 'topic_distributions' index.")


def verify_topic_distributions(df):
    """
    Verifies the topic_distribution field for video documents in Elasticsearch by querying and printing them.

    Parameters:
    - df: DataFrame containing video IDs in the 'id' column.
    """
    for video_id in df['id']:
        try:
            response = es.get(index='videos', id=video_id)
            if 'found' in response and response['found']:
                topic_distribution = response['_source'].get(
                    'topic_distribution', 'Field not found')
                print(
                    f"Video ID {video_id}: topic_distribution = {topic_distribution}")
            else:
                print(f"Video ID {video_id} not found in the database.")
        except NotFoundError:
            print(
                f"NotFoundError: Video ID {video_id} does not exist in the database.")
        except Exception as e:
            print(f"Error fetching video ID {video_id}: {e}")


def query_video_from_index_videos(video_id):
    """
    Query the elasticsearch index 'videos' for the video ID given.
    Print a message if the video ID is not in the index.
    """
    try:
        # Retrieve the document by ID from the 'videos' index
        response = es.get(index="videos", id=video_id)

        return parse_hit(response)
    except NotFoundError:
        print(f"Video ID {video_id} not found in the index 'videos'.")
        return None
    except Exception as e:
        print(f"An error occurred while querying video ID {video_id}: {e}")
        return None


def query_video_from_index_topic_distributions(video_id):
    """
    Query the elasticsearch index 'topic_distributions' for the video ID given.
    Print a message if the video ID is not in the index.
    """
    try:
        # Retrieve the document by ID from the 'videos' index
        response = es.get(index="topic_distributions", id=video_id)

        return parse_hit(response)
    except NotFoundError:
        print(f"Video ID {video_id} not found in the index 'topic_distributions'.")
        return None
    except Exception as e:
        print(f"An error occurred while querying video ID {video_id}: {e}")
        return None


def write_topic_preferences(user_id, topic_preferences):
    """
    Writes the user topic preferences to the specified user in the Elasticsearch database.

    Parameters:
    - user_id (str): The unique identifier of the user.
    - topic_preferences (list of float): The topic distribution scores to be written.

    Returns:
    - bool: True if the operation was successful, False otherwise.
    """
    try:
        # Construct the body of the update request
        body = {
            "doc": {
                "topic_preferences": topic_preferences
            }
        }

        # Update the topic preferences in the Elasticsearch index
        response = es.update(index="users", id=user_id, body=body)
        resp = es.indices.refresh(
            index="users",
        )
        # Check the response from Elasticsearch
        if response['result'] in ['updated', 'created']:
            print(f"Topic preferences for user ID {user_id} successfully updated.")
            return True
        else:
            print(f"Failed to update topic preferences for user ID {user_id}.")
            return False
    except Exception as e:
        print(f"An error occurred while updating the topic preferences: {e}")
        return False


def remove_fields_from_user(user_id, fields_to_remove):
    try:
        # Define the script to remove fields by setting them to null
        # Here we use a loop to iterate over the fields to remove
        script = {
            "source": "for (field in params.fields) { ctx._source.remove(field) }",
            "params": {
                "fields": fields_to_remove
            }
        }

        # Perform the update operation
        response = es.update(
            index="users",  # The index where your users are stored
            id=user_id,  # The unique identifier for the user
            body={"script": script}
        )
        resp = es.indices.refresh(
        index="users",
        )

        # Check response for success
        if response['result'] in ['updated', 'noop']:
            print(f"Fields {fields_to_remove} removed from user ID {user_id}.")
            return True
        else:
            print(f"Failed to remove fields from user ID {user_id}.")
            return False

    except Exception as e:
        print(f"An error occurred while updating the user profile: {e}")
        return False


def get_all_feedback_by_user_id(user_id):
    """
    Retrieves all feedback entries for a specific user from the 'feedback' index.

    Parameters:
    - user_id (str): The unique identifier of the user.

    Returns:
    - list: A list of all feedback entries for the user.
    """
    feedback_entries = []
    try:
        # Define the search query to filter by user ID
        search_query = {
            "query": {
                "term": {
                    "userId.keyword": user_id  # Use the ".keyword" for exact match
                }
            }
        }

        # Initialize the scan operation
        feedback_scan = scan(
            client=es,
            index='feedback',
            query=search_query,
        )

        # Iterate over the scan results and collect feedback entries
        feedback_entries=[entry['_source'] for entry in feedback_scan]

        return feedback_entries

    except Exception as e:
        print(
            f"An error occurred while retrieving feedback for user ID {user_id}: {e}")
        return []


def submit_video_feedback(user_id, video_id, rating):
    """
    Submits feedback about a video to the Elasticsearch database. If a feedback entry
    for the same video by the same user already exists, it overwrites the previous feedback.

    Parameters:
    - user_id (str): The unique identifier of the user submitting the feedback.
    - video_id (str): The unique identifier of the video the feedback is about.
    - rating (int): The rating given to the video. Must be 1, 0, or -1.

    Returns:
    - bool: True if the operation was successful, False otherwise.
    """
    feedback_id = f"{user_id}_{video_id}"
    timestamp = int(time.time() * 1000)  # Current timestamp in milliseconds

    feedback_doc = {
        '_class': 'de.tum.rs.dao.Feedback',
        'userId': user_id,
        'videoId': video_id,
        'rating': rating,
        'timestamp': timestamp,
    }

    try:
        response = es.update(
            index="feedback",
            id=feedback_id,
            body={
                "doc": feedback_doc,
                "doc_as_upsert": True
            }
        )
        resp = es.indices.refresh(
        index="feedback",
        )

        print(f"Feedback submitted successfully: {response}")
        return True
    except Exception as e:
        print(f"An error occurred while submitting feedback: {e}")
        return False


def remove_all_feedback_for_user(user_id):
    """
    Removes all feedback entries for a specific user from the Elasticsearch database.

    Parameters:
    - user_id (str): The unique identifier of the user whose feedback should be removed.

    Returns:
    - int: The number of feedback entries deleted.
    """
    # Define a query to match all feedback entries for the given user ID
    query = {
        "query": {
            "match": {
                "userId": user_id
            }
        }
    }

    try:
        # Perform the delete by query operation
        response = es.delete_by_query(index="feedback", body=query)

        # Extract the number of deleted documents from the response
        num_deleted = response['deleted']
        print(
            f"Successfully removed {num_deleted} feedback entries for user ID {user_id}.")
        return num_deleted
    except Exception as e:
        print(
            f"An error occurred while removing feedback for user ID {user_id}: {e}")
        return 0


def similarity_search(topic_preferences, watched_videos, k):
    """
    Performs a similarity search in Elasticsearch using the user's topic preferences,
    excluding videos the user has already watched. Returns detailed information
    for each recommended video, including the video ID and its most relevant topics.

    :param topic_preferences: List of topic preferences for the user.
    :param watched_videos: List of video IDs that the user has already watched.
    :return: List of dicts with detailed info for each recommended video.
    """


    # Construct the query
    query = {
        "query": {
            "bool": {
                "must": {
                    "script_score": {
                        "query": {"match_all": {}},
                        "script": {
                            "source": "double similarity = cosineSimilarity(params.query_vector, 'topic_distribution') + 1.0; return Double.isNaN(similarity) ? 0.0 : similarity;",
                            "params": {
                                "query_vector": topic_preferences
                            }
                        }
                    }
                },
                "must_not": {
                    "ids": {
                    "values": watched_videos  # Exclude videos already watched
                    }
                }
            }
        },
        "_source": ["video_id", "most_relevant_topics"],  # Specify fields to include in the response
        "size": k  # Adjust as needed
    }

    # Execute the query
    response = es.search(index="topic_distributions", body=query)

    # Extract detailed info for each recommended video from the hits
    results = [{
        "video_id": hit["_source"]["video_id"],
        "most_relevant_topics": hit["_source"]["most_relevant_topics"]
    } for hit in response["hits"]["hits"]]
    
    return results


def load_topic_descriptions(index_name='topics'):
    """
    Fetches topic descriptions from the Elasticsearch index.

    :param index_name: Name of the Elasticsearch index containing topics data.
    :return: A dictionary mapping each topic number to its description.
    """

    # Prepare a search query to fetch all documents from the index
    query = {
        "size": 1000,  # Adjust this value based on the expected number of topics
        "query": {
            "match_all": {}
        }
    }

    # Execute the search query
    response = es.search(index=index_name, body=query)
    
    # Parse the response to extract topic descriptions
    topic_descriptions = {}
    for hit in response['hits']['hits']:
        topic_number = hit['_source'].get('topic_number')  # Adjust field name as necessary
        description = hit['_source'].get('description')
        if topic_number is not None and description is not None:
            topic_descriptions[topic_number] = description

    return topic_descriptions
    

def get_topic_description(topic_index):
    """
    Fetches the description of a given topic index from the Elasticsearch 'topics' index.

    :param es: Elasticsearch client instance.
    :param topic_index: The index of the topic for which to fetch the description.
    :return: The description of the topic, or None if the topic is not found.
    """
    query_body = {
        "query": {
            "term": {"topic_number": topic_index}
        },
        "size": 1
    }
    
    # Assuming 'topics' is the name of your index containing the topic descriptions
    response = es.search(index="topics", body=query_body)
    
    if response['hits']['total']['value'] > 0:
        # Assuming there's only one document per topic index
        description = response['hits']['hits'][0]['_source'].get('description', None)
        return description
    else:
        return None
    

def get_durations_bulk(video_ids):
    """
    For the given video ids, make a bulk call to index 'videos' and get the video durations.
    Return them as a list in the same order as the ids.
    """
    if not video_ids:
        return []
    
    # Prepare the multi-get body
    mget_body = {
        "docs": [
            {
                "_id": video_id,
                "_source": ["contentDetails.duration"]  # Specify the fields you want to return here
            }
            for video_id in video_ids
        ]
    }

    response = es.mget(index="videos", body=mget_body)

    durations = [doc['_source']['contentDetails']['duration'] if doc['found'] else None for doc in response['docs']]
    return durations


def get_topic_distributions_bulk(video_ids):
    """
    For the given video ids, make a bulk call to index 'topic_distributions' and get the topic distributions.
    Return them as a list in the same order as the ids.
    """
    # Prepare the multi-get body
    mget_body = {
        "docs": [
            {
                "_id": video_id,
                "_source": ["topic_distribution"]
            }
            for video_id in video_ids
        ]
    }

    response = es.mget(index="topic_distributions_test", body=mget_body)

    durations = [doc['_source']['topic_distribution'] if doc['found'] else None for doc in response['docs']]
    return durations


def update_exploit_coeff(user_id, new_coeff):
    """
    Updates the 'exploit_coeff' field for a specific user in the 'users' Elasticsearch index.

    Parameters:
    - user_id: The ID of the user for whom to update the exploit coefficient.
    - new_coeff: The new value to set for the 'exploit_coeff' field.
    """
    # Update the document in Elasticsearch
    try:
        response = es.update(
            index="users",
            id=user_id,  # Assuming user_id corresponds to the document ID
            body={
                "doc": {
                    "exploit_coeff": new_coeff
                }
            }
        )
        resp = es.indices.refresh(
        index="users",
        )
    except Exception as e:
        print(f"An error occurred: {e}")


def read_disliked_video_ids(user_id):
    """
    Retrieves all video IDs from the 'disliked_creators_video_ids' field for a specified user in the 'users' index.

    Parameters:
    - user_id: The ID of the user whose complete list of disliked video IDs are to be retrieved.

    Returns:
    - A list of video IDs that the user has disliked, or an empty list if none are found.
    """
    try:
        # Fetch the full document to ensure no API defaults limit our field data
        doc = es.get(index="users", id=user_id, _source="disliked_creators_video_ids")
        if doc['found'] and 'disliked_creators_video_ids' in doc['_source']:
            return doc['_source']['disliked_creators_video_ids']
        else:
            print("No disliked video IDs field found for the user.")
            return []
    except Exception as e:
        print(f"An error occurred: {e}")
        return []
