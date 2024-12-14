import sys
sys.path.append('/Users/pablojerezarnau/git/RS-backend/')

from Modules.database_queries import es


def upload_topic_ratings(user_id, topic_ratings):
    """
    Updates the topic ratings for a specific user in the Elasticsearch index 'users'.
    
    :param user_id: The user's ID.
    :param topic_ratings: A dictionary containing the user's topic ratings.
    :return: A status message indicating the outcome of the operation.
    """
    try:
        # Construct the update body with the topic ratings structured as per the index mapping
        update_body = {
            "doc": {
                "topic_ratings": {
                    "liked": topic_ratings.get("liked", []),
                    "unrated": topic_ratings.get("unrated", [])
                }
            }
        }

        # Update the user document with the new topic ratings
        response = es.update(index="users", id=user_id, body=update_body)
        resp = es.indices.refresh(
            index="users",
        )
        # Check if the update was successful
        if response['result'] in ['updated', 'noop']:
            return f"Topic ratings for user {user_id} updated successfully."
        else:
            return f"Failed to update topic ratings for user {user_id}."
    except Exception as e:
        return f"Error updating topic ratings for user {user_id}: {str(e)}"


def execute_query(percentile_window, most_relevant_topics, second_most_relevant_topics, exclude_video_ids):
    """
    Executes a search query on the 'topic_distributions' index with dynamic parameters, excluding certain video IDs, and sorts the results.

    :param es: Elasticsearch client instance.
    :param percentile_window: Tuple containing the lower and upper bounds of the percentile window.
    :param most_relevant_topics: List of topic indexes considered for the first filter.
    :param second_most_relevant_topics: List of topic indexes considered for the second filter.
    :param exclude_video_ids: List of video IDs to exclude from the search results.
    """

    query_body = {
        "query": {
            "bool": {
                "must": [
                    {
                        "nested": {
                            "path": "most_relevant_topics_dict",
                            "query": {
                                "bool": {
                                    "should": [
                                        {
                                            "terms": {
                                                "most_relevant_topics_dict.1.topic_index": most_relevant_topics
                                            }
                                        }
                                    ],
                                    "must": [
                                        {
                                            "range": {
                                                "most_relevant_topics_dict.1.percentile": {
                                                    "gte": percentile_window[0],
                                                    "lte": percentile_window[1]
                                                }
                                            }
                                        }
                                    ],
                                    "minimum_should_match": 1
                                }
                            }
                        }
                    },
                    {
                        "nested": {
                            "path": "most_relevant_topics_dict",
                            "query": {
                                "bool": {
                                    "filter": [
                                        {
                                            "terms": {
                                                "most_relevant_topics_dict.2.topic_index": second_most_relevant_topics
                                            }
                                        }
                                    ]
                                }
                            }
                        }
                    }
                ],
                "must_not": [
                    {
                        "ids": {
                            "values": exclude_video_ids  # This line excludes specific video IDs
                        }
                    }
                ]
            }
        }
    }

    response = es.search(index="topic_distributions", body=query_body)
    return response
