from flask import Flask,request,jsonify
import Modules.database as database
import Modules.RecommenderEngine as recommenderEngine
import time
import re
from Modules.database_queries import es

app = Flask(__name__)


PYTHON_SERVICE_URL='http://127.0.0.1:5000'
	

# topicController
@app.route("/topics", methods=['GET'])
def getTopics():
    return jsonify(database.load_topics()), 200


@app.route("/topics/<userId>", methods=['POST'])
def initializeTopics(userId):
    topicIds=request.get_json()
    try:
        print("Initializing topics {} for user: {}".format(topicIds, userId));
        recommenderEngine.regiserUser(userId, topicIds)
    except:
        print ("Error while initializing topics for user: {}, Deleted user: {}".format(userId,userId))
        database.deleteByUserId(userId)
        return "Error while initializing topics for user: " + userId + " Please try again later!",500
    return "Topics initialized successfully for user: " + userId,200

# LocalVideoController
@app.route("/videos/recommendations", methods=['GET'])
def getRecommendations():
    userId=request.args.get('userId', None)
    recommendations=[]
    if (userId==None):
        print("User {} does not logged in".format(userId))
        return "Not logged in",400
    user=database.findByUserId(userId)
    if(user != None):
        recommenderEngine.invokeProcessFeedback(user)
        recommendations=recommenderEngine.getRecommendations(userId)
        print(recommendations)
        
        mget_body = {
            "docs": [
                {
                    "_id": recommendation["videoId"],  # Specify the fields you want to return here
                }
                for recommendation in recommendations
            ]
        }

        response = es.mget(index="videos", body=mget_body)
        for recommendation,doc in zip(recommendations,response['docs']):
            recommendation["video"]=doc['_source']
        return recommendations,200
    return "Error",400

def addTopics(videoId):
    topicDistribution = database.findtopicDistributionById(videoId)
    if (topicDistribution!=None):
        topics=topicDistribution["most_relevant_topics"]
        topicResult=[]
        mget_body = {
            "docs": [
                {
                    "_id": topicScore["topic_index"],  # Specify the fields you want to return here
                }
                for topicScore in topics
            ]
        }

        response = es.mget(index="topics", body=mget_body)
        for doc in response['docs']:
            topic=doc['_source']
            topicResult.append({
                    "score":topicScore["score"],
                    "id":topic["topic_number"],
		            "description":topic["description"]
                })
        return topicResult
    return None

@app.route("/videos/search", methods=['GET'])
def searchVideos():
    keyword=request.args.get('keyword', None)
    page=request.args.get('page', None)
    videos=database.findVideoByKeyword(keyword,page)
    recommendations=[]
    mget_body = {
        "docs": [
            {
                "_id": recommendation["videoId"],  # Specify the fields you want to return here
            }
            for recommendation in recommendations
        ]
    }

    response = es.mget(index="videos", body=mget_body)
    for video,doc in zip(videos,response['docs']):
        recommend={
            "videoId":video["id"],
            "explanation":"Search results",
            "video":doc['_source'],
            "topics":addTopics(video["id"])
        }
        recommendations.append(recommend)
    return recommendations,200
 

# userController
@app.route("/users/login", methods=['POST'])
def loginUser():
    userId=request.get_json()["userId"]
    print("User: {}".format(userId))
    if (database.findByUserId(userId) != None):
        return "Login successful!",200
    return "User does not exist!",400


@app.route("/users/register", methods=['POST'])
def regiserUser():
    userId=request.get_json()["userId"]
    answers=request.get_json()["answers"]
    if (userId == None or userId==""):
        print("User ID cannot be empty!")
        return "User ID cannot be empty!",400
    if (not re.match(r"^[a-zA-Z0-9]*$",userId)):
        print("User ID can only contain alphanumeric characters!")
        return "User ID can only contain alphanumeric characters!",400
    
    if (database.findByUserId(userId) == None):
        user={
            "feedbackLastUsed":int(time.time()),
            "userId":userId,
            "registrationDate":int(time.time()),
            "answers":answers
        }
        database.saveUser(user)
        print("User {} registered successfully!".format(userId))
        return "Registration successful!",200
    return "User already exists!",400

@app.route("/users/<userId>", methods=['GET','POST'])
def getUser(userId):
    user=database.findByUserId(userId)
    if(user != None):
        if request.method == 'GET': 
            recommenderEngine.invokeProcessFeedback(user)
            print("Loading User profile from database for user {}", user["userId"])
            user=database.findByUserId(userId)
            print(user)
            userDTO = {
                "userId":user["userId"],
                "n_recs_per_model":user["n_recs_per_model"],
                "exploit_coeff":user["exploit_coeff"]
            }

            # find top 10 topics
            top10TopicDto = []
            for topicId, score in user["processed_topic_scores"].items():
                topic=database.findTopicById(topicId)

                topicDTO = {
                    "score":score,
                    "id":topic["topic_number"],
                    "description":topic["description"]
                }
                top10TopicDto.append(topicDTO)
            
            userDTO["topic_preferences"]=top10TopicDto
            print("User {} retrieved successfully!".format(userId))
            return jsonify(userDTO),200

        if request.method == 'POST':
            newUser=request.get_json()
            user["exploit_coeff"]=newUser["exploit_coeff"]
            user["n_recs_per_model"]=newUser["n_recs_per_model"]
            topicDTOs=newUser["topic_preferences"]
            top10Topics={topicDTO["id"]:topicDTO["score"] for topicDTO in topicDTOs}
            print(top10Topics)
            user["processed_topic_scores"]=top10Topics
            res=database.saveUser(user)
            try:
                recommenderEngine.invokeUpdateModel(userId)
            except Exception as e:
                print(e)
            print("User {} updated successfully!".format(userId))
            return "User updated successfully!",200

            
    print("User {} does not exist!".format(userId))
    return "User does not exist!",400

#feedbackController
@app.route("/feedback", methods=['POST'])
def saveFeedback():
    feedbacks=request.get_json()
    for feedback in feedbacks:
        feedback["id"]=feedback["userId"] + "_" + feedback["videoId"]
        res=database.saveFeedback(feedback)
    print("Saved feedbacks")
    return "",200

#interactionController
@app.route("/interactions", methods=['POST'])
def saveInteraction():
    
    interactions=request.get_json()
    for interaction in interactions:
        database.saveInteraction(interaction)
    print("Saved feedbacks")
    return "",200
   
if __name__ == '__main__':
    app.run(debug=True, port=8081)


