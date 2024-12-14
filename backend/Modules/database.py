# from Modules import helper_functions
from elasticsearch import Elasticsearch
import random

# Define database url and credentials
url = "http://localhost:9200/"
username = "elastic"
password = "gamUBg0KZZ0w5i6tikd0"

# Setup the connection to Elasticsearch
es = Elasticsearch(
    url,
    basic_auth=(username, password)
)

def load_topics(index_name='topics'):
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
        },
         "fields": ["topicNumber","description","document_count","tokens"]
    }

    # Execute the search query
    response = es.search(index=index_name,body=query)
    topics = []
    for hit in response['hits']['hits']:
        topic_number = hit['_source'].get('topic_number')  # Adjust field name as necessary
        description = hit['_source'].get('description')
        if topic_number is not None and description is not None:
            temp={
                'id':topic_number,
                'topicNumber':topic_number,
                'description':description
            }
            topics.append(temp)
    random.shuffle(topics)
    topics=topics[:80]
    return topics

def deleteByUserId(userId,index_name='users'):
    # Perform the delete by query operation
    response = es.delete(index=index_name, id=userId)


def findByUserId(userId,index_name='users'):
    if es.exists(index=index_name, id=userId):
        return es.get(index=index_name, id=userId)['_source']
    else:
        return None
    

def saveUser(user,index_name='users'):
    res = es.index(index=index_name, id=user['userId'], body=user)
    resp = es.indices.refresh(
        index=index_name,
    )

    return resp

def findByUserIdAndTimestampGreaterThan(userId, feedbackLastUsed,index_name='feedback'):
    search_query = {
        "bool": {
            "must": [
                {
                    "term": {
                        "userId": userId  # Use the ".keyword" for exact match
                    }
                },
                {
                    "range" : {
                        "timestamp" : {
                            "gt" : feedbackLastUsed,
                        }
                    }
                }
                
            ]
        }
    }
    
    resp = es.search(index=index_name,query=search_query)
    feedbacks = [hit['_source'] for hit in resp['hits']['hits']]
    return feedbacks

def findTopicById(topicId,index_name='topics'):
    topic=es.get(index=index_name, id=topicId)['_source']
    return topic

def findVideoById(videoId,index_name='videos'):
    video=es.get(index=index_name, id=videoId)['_source']
    return video

def findVideoByKeyword(keyword,page,index_name='videos'):
    search_query = {
        "bool": {
            "should": [
                    {
                        "wildcard": {
                            "snippet.title": keyword  # Use the ".keyword" for exact match
                        }
                    },
                    {
                        "wildcard" : {
                            "snippet.description" : keyword
                        }
                    },
                    {
                        "wildcard" : {
                            "snippet.tags" : keyword
                        }
                    }
                
            ],
            "minimum_should_match":1
        }
    }
    
    resp = es.search(index=index_name,from_=page,query=search_query,size=20)
    videos = [hit['_source'] for hit in resp['hits']['hits']]
    return videos

def findtopicDistributionById(videoId,index_name='topic_distributions'):
    topicDistribution=es.get(index=index_name, id=videoId)['_source']
    return topicDistribution


def saveFeedback(feedback,index_name="feedback"):
    res = es.index(index=index_name, id=feedback['id'], body=feedback)
    resp = es.indices.refresh(
        index=index_name,
    )

    return resp


def saveInteraction(interaction,index_name="interaction"):
    res = es.index(index=index_name, body=interaction)
    resp = es.indices.refresh(
        index=index_name,
    )

    return resp


    

    


