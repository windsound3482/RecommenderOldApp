import sys
sys.path.append('/Users/pablojerezarnau/git/RS-backend/')

from Modules import rs_logic
from Modules.topic_preferences_management import read_topic_preferences_of_user, database_queries
from collections import Counter


def generate_individual_explanation(most_relevant_topics, topic_descriptions):
    """
    Generates an explanation for a video based on its most relevant topics, with each topic description
    surrounded by single quotes.
    
    :param most_relevant_topics: A list of dictionaries, each containing 'topic_index' and its score.
    :param topic_descriptions: A dictionary mapping topic numbers to their descriptions.
    :return: A string containing the video's explanation.
    """
    # Sort the most relevant topics by score and take the top 2
    top_topics = sorted(most_relevant_topics, key=lambda item: item.get('score', 0), reverse=True)[:2]

    # Build explanations with each description surrounded by single quotes
    explanations = [f"'{topic_descriptions.get(topic['topic_index'], 'No description available')}'" for topic in top_topics]

    # Join the top topic descriptions for the final explanation
    explanation_str = " and ".join(explanations)
    return f"Recommended to you because you seem to like videos with the topics {explanation_str}."


def generate_group_explanation(similarity_results, topic_descriptions):
    """
    Generates a personalized group explanation mentioning the top 3 most frequent topics.
    
    :param topic_frequencies: A Counter object with topic frequencies.
    :param topic_descriptions: A dictionary mapping topic numbers to their descriptions.
    :return: A string containing the group explanation.
    """

    def aggregate_topic_frequencies(similarity_results):
        """
        Aggregates frequencies of topic indices across all recommended videos.
        
        :param similarity_results: List of dicts, each with 'video_id' and 'most_relevant_topics'.
        :return: A Counter object with topic frequencies.
        """
        all_topics = []
        for result in similarity_results:
            most_relevant_topics = result['most_relevant_topics']
            # Assuming most_relevant_topics is a list of dictionaries with 'topic_index'
            topic_indices = [topic['topic_index'] for topic in most_relevant_topics]
            all_topics.extend(topic_indices)

        return Counter(all_topics)

    # Aggregate topic frequencies and generate group explanation
    topic_frequencies = aggregate_topic_frequencies(similarity_results)

    # Identify top 5 topics
    top_five_topics = topic_frequencies.most_common(3)
    
    # Fetch and format descriptions of the top five topics
    top_descriptions = [f"'{topic_descriptions.get(topic_index, 'No description available')}'" for topic_index, _ in top_five_topics]
    
    # Construct group explanation string
    if top_descriptions:
        topics_str = ", ".join(top_descriptions[:2]) + ", and " + top_descriptions[-1]
        return f"These videos are recommended based on your interest in specific topics: {topics_str}."
    else:
        return "These videos are tailored to your interests, focusing on topics you've engaged with recently."


def get_recommendations(user_id, n_recs):
    """
    Generates personalized video recommendations for a given user.

    Parameters:
    - user_id (str): The unique identifier of the user for whom recommendations are being generated.
    - n_recs (int): The number of recommendations to generate.

    Returns:
    - list of dicts: Each dictionary contains:
        - 'video_id' (str): The unique identifier of the recommended video.
        - 'explanation' (str): Explanation of why the video is recommended.
      The last dictionary in the list provides a 'group' explanation applicable to all recommendations.

    The recommendations are based on the user's topic preferences and previously watched videos, ensuring relevance and novelty.
    """
    
    # Retrieve descriptions for all topics
    topic_descriptions = database_queries.load_topic_descriptions()

    # Determine the user's topic preferences
    topic_preferences = read_topic_preferences_of_user(user_id=user_id)

    # Identify videos already watched by the user to exclude from recommendations
    watched_videos = rs_logic.get_videos_rated_by_user(user_id=user_id)

    # Conduct a similarity search
    similarity_results = database_queries.similarity_search(topic_preferences=topic_preferences, watched_videos=watched_videos, k=n_recs)
    
    recommendations = []
    # Create personalized explanations for each recommended video.
    for result in similarity_results:
        video_id = result["video_id"]
        most_relevant_topics = result["most_relevant_topics"]
        explanation = generate_individual_explanation(most_relevant_topics, topic_descriptions)
        recommendations.append({"videoId": video_id, "explanation": explanation, "model": "similarity search"})

    return recommendations
