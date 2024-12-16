import sys
sys.path.append('/Users/pablojerezarnau/git/RS-backend/')

from Modules import database,topic_preferences_management, database_queries, helper_functions, topic_categories_management, additional_rating_options
import numpy as np
from Modules.database_queries import es
import config


# Map explicit video ratings to update weights
explicit_rating_weights = {
    1: -2,
    2: -1,
    3: 0.5,
    4: 1,
    5: 2
}


def print_feedback_list(feedback_list):
    """
    Processes a list of feedback data entries to update the recommendation system model.
    Prints each feedback's user ID, video ID, rating, 'more', and 'less' fields.
    Returns a list of feedback processing messages if successful, otherwise returns False if any required fields are missing.

    :param feedback_list: A list of dictionaries, each containing feedback information from a user.
    :return: A list of strings indicating the processing status of each feedback entry, or False if any required fields are missing.
    """
    response = []  # Initialize an empty list to store processing messages

    for feedback_entry in feedback_list:
        # Check for required fields and return False if any are missing
        if 'userId' not in feedback_entry or 'videoId' not in feedback_entry or 'rating' not in feedback_entry or 'more' not in feedback_entry or 'less' not in feedback_entry:
            print("Some feedback entries have missing required fields.")
            return False  # Stop processing if any required field is missing
        
        print(f'entire feedback_entry: {feedback_entry}')

        # Extract fields with validation passed
        user_id = feedback_entry['userId']
        video_id = feedback_entry['videoId']
        rating = feedback_entry['rating']
        more = feedback_entry['more']
        less = feedback_entry['less']

        # Construct a message for the current feedback entry
        single_response = (f"User '{user_id}' on video '{video_id}':\n"
                           f"\tRating: '{rating}', More: '{more}', Less: '{less}'.")
        print(single_response)  # Pretty print the feedback information
        response.append(single_response)

    return response  # Return the list of feedback processing messages


def get_necessary_watch_ratios(feedback_list):
    """
    Evaluate which feedback entries do not have an explicit rating. For those entries, query the
    index 'videos' and get the video duration and then calculate the watch ratio. Add the key 'watch_ratio'
    to those feedback entries.
    """
    # Check if list is empty
    if not feedback_list:
        return feedback_list

    # Evaluate for which feedback entries there is no explicit rating
    ids_ex_rating_missing = [feedback['videoId'] for feedback in feedback_list if feedback['rating'] == 0]
    print(f'Feedback processing - Getting necessary watch ratios - ids_ex_rating_missing: {ids_ex_rating_missing}')
        
    # Get and format video durations
    unformatted_durations = database_queries.get_durations_bulk(ids_ex_rating_missing)
    print(f'Feedback processing - Getting necessary watch ratios - unformatted_durations: {unformatted_durations}')    

    # Convert the ISO 8601 durations to seconds
    durations_in_seconds = {video_id: helper_functions.iso8601_duration_to_seconds(duration)
                            for video_id, duration in zip(ids_ex_rating_missing, unformatted_durations)}
    print(f'Feedback processing - Getting necessary watch ratios - durations_in_seconds: {durations_in_seconds}')

    # Calculate watch ratios and add to feedback_list
    for feedback in feedback_list:
        if feedback['rating'] == 0:
            video_id = feedback['videoId']
            watch_time = feedback['totalWatchTime']  # Assuming 'watch_time' is in seconds
            video_duration = durations_in_seconds.get(video_id, 0)  # Get duration or default to 0

            # Avoid division by zero if duration is not found or invalid
            if video_duration > 0:
                watch_ratio = watch_time / video_duration
                feedback['watchRatio'] = watch_ratio

    return feedback_list


def keep_necessary_feedback_entries(feedback_list):
    """
    Keep only entries where explicit rating is given, watch ratio is over 50%, 
    topic ratings are given, dislikeReasons are given.
    """
    new_feedback_list = []
    for feedback in feedback_list:
        print(f'Evaluating if video {feedback.get("videoId")} needs processing.')
        
        # Check all conditions safely using .get() to avoid KeyError
        rating = feedback.get('rating', 0) != 0
        watch_ratio = feedback.get('watchRatio', 0) > 0.5
        topic_ratings = feedback.get('more') or feedback.get('less')
        dislike_reasons = feedback.get('dislikeReasons')

        # If any condition is True, skip to the next feedback
        if rating or watch_ratio or topic_ratings or dislike_reasons:
            new_feedback_list.append(feedback)
            print(f'id: {feedback.get("videoId")} is kept for processing.')
        else:
            print(f'id: {feedback.get("videoId")} does not meet criteria and is removed.')

    print(f'Number of feedback entries where processing is necessary: {len(new_feedback_list)}')
    return new_feedback_list



def get_necesary_topic_distributions(feedback_list):
    """
    Given the ids of the videos with explicit rating or watch ratio over 0.5,
    make a bulk request to index 'topic_distributions' and retrieve the topic distributions.
    Then, add the field 'topicDistribution' in the corresponding dicts.
    """
    # Identify videos based on given conditions
    video_ids = [feedback['videoId'] for feedback in feedback_list if feedback.get('rating', 0) != 0 or
                 feedback.get('watchRatio', 0) > 0.5 or
                 'Not interested in topics' in feedback.get('dislikeReasons', [])]
    print(f'Process feedback: video ids to query topic distributions: {video_ids}')

    # If list is empty, avoid processing
    if not video_ids:
        return feedback_list
    
    # Retrieve topic distributions for these video IDs in bulk
    topic_distributions = database_queries.get_topic_distributions_bulk(video_ids)
    
    # Map each videoId to its corresponding topic distribution for easier access
    video_id_to_topic_distribution = dict(zip(video_ids, topic_distributions))
    
    # Enrich the feedback_list with topicDistribution data
    for feedback in feedback_list:
        video_id = feedback.get('videoId')
        if video_id in video_id_to_topic_distribution:
            feedback['topicDistribution'] = video_id_to_topic_distribution[video_id]
    
    return feedback_list


def update_with_feedback(topic_preferences, feedback):
    """
    Update the topic preferences vector according to the information in the dict feedback.
    Apply the video rating and the topic rating.
    """
    video_id = feedback.get('videoId')
    print(f'Feedback processing - Processing individual feedback entries - Video rating - video_id: {video_id}')
    video_rating = feedback.get('rating')
    watch_ratio = feedback.get('watchRatio')
    topic_distribution = feedback.get('topicDistribution')
    topic_preferences_np = np.array(topic_preferences)
    topic_distribution_np = np.array(topic_distribution)
    # If explicit rating given
    if video_rating and video_rating != 0:
        # Get rating weight
        weigth = explicit_rating_weights[video_rating]
        # Calculate the weighted average
        updated_preferences = 0.9 * topic_preferences_np + 0.1 * weigth * topic_distribution_np
        # Ensure all values are at least 0
        updated_preferences = np.maximum(updated_preferences, 0)
        topic_preferences = updated_preferences.tolist()
    # If watch ratio over 50%
    elif watch_ratio and watch_ratio > 0.5:
        # Get rating weight
        weigth = 0.5
        if 0.75 <= watch_ratio <= 100:
            weigth = 1
        # Calculate the weighted average
        updated_preferences = 0.9 * topic_preferences_np + 0.1 * weigth * topic_distribution_np
        topic_preferences =updated_preferences.tolist()
    print(f'Feedback processing - Processing individual feedback entries - Video rating')
    update_sum = 0.1
    update_factor = 0.5

    more_topics = [int(topic) for topic in feedback.get('more', [])]
    less_topics = [int(topic) for topic in feedback.get('less', [])]

    for topic_index in more_topics:
        if 0 <= topic_index < len(topic_preferences):
            topic_preferences[topic_index] += update_sum

    for topic_index in less_topics:
        if 0 <= topic_index < len(topic_preferences):
            topic_preferences[topic_index] *= update_factor

    print(f'Feedback processing - Processing individual feedback entries - Topic rating')

    print(f'Feedback processing - Processing individual feedback entries - Dislike topics (additional options)')
    if 'Not interested in topics' in feedback.get('dislikeReasons', []):
        # Get the indices of the top 3 scoring topics from the video's topic distribution
        # We sort the indices based on their scores, taking the top 3
        top_three_indices = sorted(range(len(feedback['topicDistribution'])), key=lambda i: feedback['topicDistribution'][i], reverse=True)[:3]

       
        # Reduce the score for these topics by half in the user's preferences
        for index in top_three_indices:
            if index < len(topic_preferences):  # Ensure we do not go out of range of the user's preferences
                topic_preferences[index] *= 0.5

    return topic_preferences




def update_topic_preferences_from_feedback(user_id, feedback_list,user_data):
    """
    Given the list of feedback entries, update the user's toic_preferences.
    """
    print(f'feedback_list: {feedback_list}')

    # 1. Get the user's topic preferences
    print('Feedback processing - Getting topic_preferences.')
    if user_data and 'topic_preferences' in user_data:
        # Extract and return the 'topic_preferences' field from the user data
        topic_preferences=user_data['topic_preferences']

        # 2. Get necessary watch ratios
        print('Feedback processing - Getting necessary watch ratios.')
        feedback_list = get_necessary_watch_ratios(feedback_list)

        # 3. Keep only entries that need processing
        print('Feedback processing - Keeping only entries that need processing.')
        feedback_list = keep_necessary_feedback_entries(feedback_list)

        # 4. Get necessary topic distributions
        print('Feedback processing - Getting necessary topic distributions.')
        feedback_list = get_necesary_topic_distributions(feedback_list)

        # 5. Process feedback
        print('Feedback processing - Processing individual feedback entries.')
        for feedback in feedback_list:
            topic_preferences = update_with_feedback(topic_preferences, feedback)
            # print(f'Fedback n. {idx}. topic_preferencse: {topic_preferences}')

        # Process the feedback field 'dislikeReasons'
        additional_rating_options.process_disliked_creators(feedback_list, user_id)

        additional_rating_options.process_too_much_similar_content(feedback_list, user_id)

        # 6. Normalise topic_preferences
        print(f'Feedback processing - Normalising updated_topic_preferences')
        topic_preferences = np.array(topic_preferences)
        topic_preferences = list(topic_preferences/topic_preferences.sum())
        
        return topic_preferences


def process_feedback(feedback_list):
    """
    Orchestrate the processing of a list of feedback elements. Triggered by the 
    POST request /feedback.
    """
    if feedback_list:
        user_id = feedback_list[0]['userId']
    else:
        return "No valid userId given."

    user_data = database.findByUserId(user_id)
    
    if user_data and 'topic_preferences' in user_data:
        # 1. Update topic_preferences from feedback
        topic_preferences=update_topic_preferences_from_feedback(user_id=user_id,
                                            feedback_list=feedback_list,user_data=user_data)
        
        topic_categories = topic_categories_management.calculate_topic_categories(user_id)
        # print(f'topic_categories: {topic_categories}')


        # 3. Update processed_topic_scores
        print('Feedback processing - Updating processed_topic_scores')
        print('Feedback processing - Updating feedbackLastUsed')
        # Extract and return the 'topic_preferences' field from the user data
        processed_topic_scores = topic_preferences_management.processed_topic_scores_from_topic_preferences(topic_preferences=topic_preferences) 
        print('Feedback processing - Updating topic_preferences')
       
        
        # 4. Update the field feedbackLastUsed in database
        timestamp=max([feedback['timestamp']for feedback in feedback_list])
        print(processed_topic_scores)
        update_script = {
            "script": {
                "lang": "painless",
                "source": "ctx._source.remove('topic_categories');ctx._source.topic_categories = params.topic_categories; \
                           ctx._source.remove('processed_topic_scores');ctx._source.processed_topic_scores = params.processed_topic_scores; \
                           ctx._source.remove('feedbackLastUsed');ctx._source.feedbackLastUsed= params.feedbackLastUsed; \
                            ctx._source.remove('topic_preferences');ctx._source.topic_preferences = params.topic_preferences; ",
                "params": {
                    "topic_categories": topic_categories,
                    "processed_topic_scores": processed_topic_scores,
                    "feedbackLastUsed": timestamp,
                    "topic_preferences": topic_preferences
                }
            }
        }
        print(timestamp)

        try:
            # Execute the update operation with the script
            response = es.update(index="users", id=user_id, body=update_script)
            resp = es.indices.refresh(
            index="users",
        )
            print(response)
            print(f"Updated user {user_id} with new processed_topic_scores.")
        except Exception as e:
            print(f"An error occurred while updating user {user_id}: {e}")

    else:
        print(f"No topic preferences found for user ID {user_id}.")
    print(f'Feedback processed successfully.')

    return "Feedback processed successfully."
