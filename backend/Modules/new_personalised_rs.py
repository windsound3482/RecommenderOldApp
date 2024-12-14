import sys
sys.path.append('/Users/pablojerezarnau/git/RS-backend/')

from Modules import topic_categories_management, topic_preferences_management, database_queries, rs_logic
from Modules.database_queries import es
import random
import numpy as np

def sample_explorative_topics(n_recs_explore, topic_categories):
    """
    Sample topics evenly from the user's "unrated" and "rated_but_not_most_liked" topic categories.
    The sampling is random within each category and attempts to distribute the samples evenly across both.

    :param n_recs_explore: Number of explorative recommendations to generate.
    :param topic_categories: Dictionary with topic categories containing "unrated" and "rated_but_not_most_liked" lists.
    :return: List of sampled topic indices.
    """
    sampled_topics = []

    # Check for empty list
    if n_recs_explore == 0:
        return sampled_topics

    # Separate the two categories
    unrated_topics = topic_categories.get('unrated', [])
    rated_not_most_liked_topics = topic_categories.get('rated_but_not_most_liked', [])

    # Determine the number of topics to sample from each category
    n_samples_each = n_recs_explore // 2
    extra_sample = n_recs_explore % 2  # In case of an odd number of recs, one category will get an extra sample

    # Sample from 'unrated' topics
    if n_samples_each <= len(unrated_topics):
        sampled_topics.extend(random.sample(unrated_topics, n_samples_each))
    else:
        sampled_topics.extend(unrated_topics)  # Not enough topics, take them all

    # Sample from 'rated_but_not_most_liked' topics
    if n_samples_each + extra_sample <= len(rated_not_most_liked_topics):
        sampled_topics.extend(random.sample(rated_not_most_liked_topics, n_samples_each + extra_sample))
    else:
        sampled_topics.extend(rated_not_most_liked_topics)  # Not enough topics, take them all

    # In case one of the lists was shorter and we didn't get enough samples, fill in the rest
    while len(sampled_topics) < n_recs_explore:
        # Select the pool that still has topics remaining
        remaining_pool = unrated_topics if len(unrated_topics) > len(rated_not_most_liked_topics) else rated_not_most_liked_topics
        # Sample the rest from the remaining pool
        sampled_topics.extend(random.sample(remaining_pool, n_recs_explore - len(sampled_topics)))

    return sampled_topics


def generate_explanation(topic, exploit_rec, exploit_coeff):
    """
    Generates explanation using the exploit_coeff and the boolean exploit_rec, which described
    whether the given recommendation is exploitative (True) or explorative (False). Also, 
    read the topic description from the database.
    """
    # Read topic description from the database (pseudo code)
    topic_description = database_queries.get_topic_description(topic_index=topic)
    
    # Formulate first part of the explanation
    if exploit_rec:
        first_part = f"Because you seem to like the topic '{topic_description}', and "
    else:
        first_part = f"Because you haven't explored the topic '{topic_description}' much yet, and "

    # Define how exploit_coeff influences the explanation
    if exploit_coeff == 1.0:
        second_part = "you want your personalised recommendations to exclusively focus on your most liked topics."
    elif exploit_coeff >= 0.8:
        second_part = "you want your personalised recommendations to heavily focus on your most liked topics." if exploit_rec else "you want your personalised recommendations to minimally feature your unexplored topics."
    elif exploit_coeff >= 0.6:
        second_part = "you want your personalised recommendations to rather focus on your most liked topics." if exploit_rec else "you want your personalised recommendations to feature some of your unexplored topics."
    elif exploit_coeff == 0.5:
        second_part = "you want your personalised recommendations to represent both your most liked and unexplored topics equally."
    elif exploit_coeff >= 0.3:
        second_part = "you want your personalised recommendations to feature some of your most liked topics." if exploit_rec else "you want your personalised recommendations to rather focus on your unexplored topics."
    elif exploit_coeff >= 0.1:
        second_part = "you want your personalised recommendations to minimally feature your most liked topics." if exploit_rec else "you want your personalised recommendations to heavily focus on your unexplored topics."
    elif exploit_coeff == 0:
        second_part = "you want your personalised recommendations to exclusively focus on unexplored topics."
    
    # Get full explanation
    explanation = first_part + second_part
    return explanation
