import sys
sys.path.append('/Users/pablojerezarnau/git/RS-backend/')

from Modules.database_queries import es, helpers
import json, os
from collections import defaultdict
from config.settings import TOPIC_MODELING_RUNS_DIR 


def process_topic_distributions(run_id):
    """
    Processes topic distributions for a given run ID. Calculates the most relevant topics and their percentiles,
    then stores this data in two different formats: 'most_relevant_topics_dict' with percentile info of the most
    relevant one, and 'most_relevant_topics' as a list of dicts with top-3 topics and their scores.
    The processed data is stored as 'topic_distributions_refined.json' in the run directory.

    :param run_id: The ID of the topic modeling run.
    :param TOPIC_MODELING_RUNS_DIR: The base directory containing topic modeling runs.
    """
    file_path = os.path.join(TOPIC_MODELING_RUNS_DIR, f"{run_id}/topic_distributions.json")
    file_out_path = os.path.join(TOPIC_MODELING_RUNS_DIR, f"{run_id}/topic_distributions_refined.json")

    # Step 1: Load videos
    videos = []
    with open(file_path, 'r') as file:
        for line in file:
            videos.append(json.loads(line))

    # Step 2: Prepare data structure for percentile calculation
    topic_videos = defaultdict(list)
    for video in videos:
        topic_distribution = video['topic_distribution']
        top_indices_scores = sorted(enumerate(topic_distribution), key=lambda x: x[1], reverse=True)[:5]
        most_relevant_topic = top_indices_scores[0][0]
        video['top_topics_info'] = [{'topic_index': index, 'topic_score': score} for index, score in top_indices_scores]
        topic_videos[most_relevant_topic].append(video)

    # Step 3: Calculate percentiles for each topic
    for topic, vids in topic_videos.items():
        vids_sorted = sorted(vids, key=lambda x: x['topic_distribution'][topic], reverse=True)
        for i, video in enumerate(vids_sorted):
            percentile = (i / len(vids_sorted)) * 100
            video['percentile'] = percentile

    # Step 4: Update videos with enriched most_relevant_topics
    for video in videos:
        enriched_most_relevant_topics_dict = {}
        most_relevant_topics = []
        for rank, topic_info in enumerate(video['top_topics_info'], start=1):
            if rank == 1:
                topic_info['percentile'] = video['percentile']
            enriched_most_relevant_topics_dict[str(rank)] = topic_info
            if rank <= 3:  # Collect top-3 topics for the new 'most_relevant_topics'
                most_relevant_topics.append({'topic_index': topic_info['topic_index'], 'score': topic_info['topic_score']})
        video['most_relevant_topics_dict'] = enriched_most_relevant_topics_dict
        video['most_relevant_topics'] = most_relevant_topics
        del video['percentile']  # Remove the temporary field
        del video['top_topics_info']  # Remove the temporary field

    # Step 5: Write the updated videos back to a new file
    with open(file_out_path, 'w') as file:
        for video in videos:
            file.write(json.dumps(video) + '\n')
    


def update_topic_distributions_pipeline(run_id):
    """
    Orchestrates the pipeline for updating topic distributions within an Elasticsearch index.
    This involves processing the topic distributions to calculate percentiles, removing old entries from
    the index, and uploading the refined data.

    :param run_id: The unique identifier for the topic modeling run.
    """
    # Step 1: Process topic distributions and save the refined data
    process_topic_distributions(run_id)
    
    # Step 2: Remove existing entries from the Elasticsearch index
    query = {"query": {"match_all": {}}}
    es.delete_by_query(index="topic_distributions", body=query)
    
    # Step 3: Upload the refined topic distributions to the Elasticsearch index
    file_path = os.path.join(TOPIC_MODELING_RUNS_DIR, f"{run_id}/topic_distributions_refined.json")

    # Generator function to read and yield documents from the file
    def generate_data():
        with open(file_path, 'r') as file:
            for line in file:
                doc = json.loads(line)
                yield {
                    "_index": "topic_distributions",
                    "_id": doc["id"],  # Assuming each document has a unique 'id' field
                    "_source": doc
                }
    
    # Use the bulk helper to upload
    helpers.bulk(es, generate_data())
    
    print("Topic distributions pipeline completed.")