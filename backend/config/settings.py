import os

# Base directory for data storage
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Directory for data
DATA_DIR = os.path.join(BASE_DIR, 'data')

# Directory for topic modeling data
TOPIC_MODELING_DATA_DIR = os.path.join(DATA_DIR, 'topic_modeling')

# Directory for topic modeling runs
TOPIC_MODELING_RUNS_DIR = os.path.join(TOPIC_MODELING_DATA_DIR, 'runs')


# Function to create a directory for a new run
def create_topic_modeling_run_dir(run_id):
    run_dir = os.path.join(TOPIC_MODELING_RUNS_DIR, run_id)
    os.makedirs(run_dir, exist_ok=True)
    return run_dir


# Topic Modeling Parameters
TOPIC_MODELING_PARAMS = {
    'num_topics': 4,
    'min_token_frequency': 15,
    'no_above': 0.1,
    'min_tokens_per_document': 10
}

# Currently TM run uploaded to the database
tm_run_in_database = '20240306_212224_standard'
num_topics_in_database = 300

# TM Dataset Analysis

# Common topic modeling parameters that do not change with num_topics
COMMON_TM_PARAMS = {
    'min_token_frequency': 15,
    'no_above': 0.10,
    'min_tokens_per_document': 10
}

# List of number of topics to test 
NUM_TOPICS_LIST = [100, 200, 300, 400]

# Define datasets with their specific filtering levels
DATASETS = {
    'layer1': 'defaultLanguage or defaultAudioLanguage is english',
    'layer2':'predictedLanguage is english',
    'layer3':'any one of defaultLanguage, defaultAudioLanguage, predictedLanguage is one of [es, fr, de, it]',
    # 'dataset1': 'layer1',
    'dataset2': 'layer1 + layer2',
    'dataset3': 'layer1 + layer2 + layer3'
}

# List of filtered out topics
filtered_topics = [13, 16, 19, 21, 27, 28, 31, 37, 43, 44, 46, 50, 54, 59, 66, 67, 68, 75, 76, 78, 80, 81, 90, 92, 96, 99, 101, 102, 103, 105, 110, 114, 122, 124, 131, 132, 136, 141, 142, 146, 147, 148, 150, 152, 154, 156, 159, 164, 166, 171, 177, 180, 184, 186, 187, 196, 199, 201, 207, 215, 217, 218, 225, 232, 237, 256, 257, 263, 264, 266, 280, 282, 283, 284, 286, 287, 292, 293, 294, 296]
