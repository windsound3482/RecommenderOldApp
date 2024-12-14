import sys
sys.path.append('/Users/pablojerezarnau/git/RS-backend/')


from Modules import database_queries
from Modules.database_queries import es

def add_disliked_creator_channel_id(user_id, channel_id):
    """
    Adds a creator's channel ID to the 'disliked_creators' list for a specific user in the 'users' index.

    Parameters:
    - user_id: The ID of the user document to update.
    - channel_id: The channel ID of the creator to add to the 'disliked_creators' list.

    Returns:
    - None, prints the outcome of the update operation.
    """
    try:
        # Use the update API with a script to append the channel_id if it's not already in the list
        response = es.update(
            index="users",
            id=user_id,  # Assuming user_id corresponds to the document ID
            body={
                "script": {
                    "source": """
                    if (ctx._source.disliked_creators == null) {
                        ctx._source.disliked_creators = [params.channel_id];
                    } else if (!ctx._source.disliked_creators.contains(params.channel_id)) {
                        ctx._source.disliked_creators.add(params.channel_id);
                    }
                    """,
                    "lang": "painless",
                    "params": {
                        "channel_id": channel_id
                    }
                }
            }
        )
        resp = es.indices.refresh(
            index="users",
        )
    except Exception as e:
        print(f"An error occurred: {e}")


def get_video_ids_from_creator(channel_id):
    """
    Retrieves all video IDs for a specific creator by querying their channel ID in the 'videos' index.

    Parameters:
    - channel_id: The channel ID of the creator whose videos need to be retrieved.

    Returns:
    - A list of video IDs, or an empty list if no videos are found.
    """
    # Prepare the query
    query = {
        "query": {
            "term": {
                "snippet.channelId.keyword": channel_id  # Use the keyword field for exact matching
            }
        },
        "_source": ["id"],  # Retrieve only the video ID field from the index
        "size": 1000  # Adjust this value based on expected results or use pagination for more
    }

    try:
        # Execute the search query
        response = es.search(index="videos", body=query)
        video_ids = [hit['_source']['id'] for hit in response['hits']['hits']]  # Extract video IDs from hits
        return video_ids
    except Exception as e:
        print(f"An error occurred when getting video IDs of a channel ID: {e}")
        return []


def add_video_ids_to_disliked(user_id, video_ids):
    """
    Appends a list of video IDs to the 'disliked_creators_video_ids' field for a specific user 
    in the 'users' index in Elasticsearch.

    Parameters:
    - user_id: The ID of the user document to update.
    - video_ids: A list of video IDs to add to the 'disliked_creators_video_ids' list.

    Returns:
    - None, prints the outcome of the update operation.
    """
    try:
        # Use the update API with a script to append each video ID if it's not already in the list
        response = es.update(
            index="users",
            id=user_id,  # Assuming user_id corresponds to the document ID
            body={
                "script": {
                    "source": """
                    if (ctx._source.disliked_creators_video_ids == null) {
                        ctx._source.disliked_creators_video_ids = params.video_ids;
                    } else {
                        for (id in params.video_ids) {
                            if (!ctx._source.disliked_creators_video_ids.contains(id)) {
                                ctx._source.disliked_creators_video_ids.add(id);
                            }
                        }
                    }
                    """,
                    "lang": "painless",
                    "params": {
                        "video_ids": video_ids
                    }
                }
            }
        )
        resp = es.indices.refresh(
            index="users",
        )
    except Exception as e:
        print(f"An error occurred: {e}")


def process_disliked_creators(feedback_list, user_id):
    """
    Processes feedbacks to identify creators that are disliked based on specific feedback entries.
    Updates the 'disliked_creators' and 'disliked_creators_video_ids' in the 'users' index.

    Parameters:
    - feedback_list: List of feedback dictionaries, each containing 'video_id' and 'feedback' keys.
    - user_id: ID of the user providing the feedback.

    Returns:
    - None
    """    
    disliked_creators = set()
    all_disliked_videos = set()

    # Process each feedback entry
    for feedback in feedback_list:
        if "Dislike the creator" in feedback['dislikeReasons']:
            video_id = feedback.get('videoId')
            # Fetch the channelId for the given video ID
            video=es.get(index="videos", id=video_id)['_source']
            channel_id = video['snippet']['channelId']
            if channel_id:
                disliked_creators.add(channel_id)
                # Fetch all videos by this creator
                video_ids = get_video_ids_from_creator(channel_id)
                print(f'Feedback processing - Additional rating - Disliked channel_id: {channel_id} with {len(video_ids)} videos.')
                all_disliked_videos.update(video_ids)
    
    # Update the user's disliked creators list
    if disliked_creators:
        for channel_id in disliked_creators:
            add_disliked_creator_channel_id(user_id=user_id,
                                            channel_id=channel_id)
    
    # Update the user's disliked creators' video IDs
    if all_disliked_videos:
        add_video_ids_to_disliked(user_id, list(all_disliked_videos))


def process_too_much_similar_content(feedback_list, user_id):
    """
    Counts how many times the string "Too much similar content" appears in the feedback list.
    For each time, reduce the exploit_coeff by 0.1.

    Parameters:
    - feedback_list: A list of feedback entries from a user.
    - user_id: The ID of the user.
    """
    # Use a list comprehension to filter and count directly
    rating_count = sum("Too much similar content" in feedback['dislikeReasons'] for feedback in feedback_list)

    # Get user's exploit_coeff
    exploit_coeff = None
    try:
        # Fetch the document for the specified user_id
        response = es.get(index="users", id=user_id)
        
        # Extract 'exploit_coeff' from the document
        exploit_coeff = response['_source'].get('exploit_coeff')

    except Exception as e:
        print(f"An error occurred while fetching 'exploit_coeff' for user {user_id}: {e}")
       

    # Calculte new exploit_coeff
    new_exploit_coeff = round(max(0, exploit_coeff - rating_count * 0.1), 1)

    # Write new exploit_coeff to index 'users'
    database_queries.update_exploit_coeff(user_id=user_id,
                                          new_coeff=new_exploit_coeff)
    
    if exploit_coeff != new_exploit_coeff:
        print(f'Feedback processing - Updated exploit_coeff of user \'{user_id}\' from {exploit_coeff} to {new_exploit_coeff}')

    return 

