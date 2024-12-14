# from Modules import helper_functions
from elasticsearch import Elasticsearch
import Modules.database as database
from Modules import process_feedback, topic_preferences_management, rs_logic


# Define database url and credentials
url = "http://localhost:9200/"
username = "elastic"
password = "gamUBg0KZZ0w5i6tikd0"
PYTHON_SERVICE_URL='http://127.0.0.1:5000'

# Setup the connection to Elasticsearch
es = Elasticsearch(
    url,
    basic_auth=(username, password)
)

def getRecommendations(userId):
    try:
        # Get recommendations
        recommendations = rs_logic.get_recommendations(userId) 
        return recommendations
    except Exception as e:
        # Log the error for debugging purposes
        print(e)
    return None

def regiserUser(userId, topicIds):
    if not userId:
        print("error: Missing userId in query parameters")

    try:
        message = rs_logic.register_user_parameters(userId, topicIds)
    except Exception as e:
        print(e)

def invokeProcessFeedback(user):
    try:
        recentFeedbacks = database.findByUserIdAndTimestampGreaterThan(user["userId"],user["feedbackLastUsed"])    
        print(recentFeedbacks)
        print("Invoking model update for User {} with {} Feedbacks".format(user["userId"], len(recentFeedbacks)))
        # Log
        print(f'Starting feedback processing with {len(recentFeedbacks)} feedback entries.')
        # Process the feedback data to update the RS model
        try:
            processing_message = process_feedback.process_feedback(feedback_list=recentFeedbacks)

            if processing_message:
                
                # Return a success response if processing is successful
               print('message: Feedback processed successfully: {processing_message}')
            else:
                # Handle processing failure
                print ('error: Failed to process feedback.')
        except Exception as e:
        # Handle potential errors in processing
            return print(e)
    except Exception as e:
        print("error while update users")
        print(e)

def invokeUpdateModel(userId):
    # If userId is wrapped in quotes, strip them
    user_id = userId.strip('"')
    print(f'POST /model with user_id \'{user_id}\'')

    if not user_id:
        print ("error: Missing userId in query parameters")
    
    try:
        message = topic_preferences_management.update_topic_preferences_from_processed_topic_scores(user_id=user_id)
    except Exception as e:
        print(e)


    


