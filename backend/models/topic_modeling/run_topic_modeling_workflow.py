import sys

sys.path.append('/Users/pablojerezarnau/git/RS-backend/')

from Modules.helper_functions import format_duration
import logging
from Modules import topic_modeling, topic_modeling_file_management
import os
from datetime import datetime
from config.settings import TOPIC_MODELING_PARAMS, create_topic_modeling_run_dir
import time
from config.settings import TOPIC_MODELING_DATA_DIR, DATASETS, NUM_TOPICS_LIST, COMMON_TM_PARAMS
import argparse

# Parse command-line arguments
parser = argparse.ArgumentParser()
parser.add_argument("--analysis-mode", help="Run the script in analysis mode", action="store_true")
args = parser.parse_args()


def setup_logger(run_dir, run_id):
    # Create a unique logger name using the run_id
    logger_name = f"{__name__}_{run_id}"
    logger = logging.getLogger(logger_name)
    
    # If the logger has handlers, remove them (this prevents duplicate logging)
    logger.handlers = []

    # Set logger level
    logger.setLevel(logging.INFO)
    
    # Create file handler
    log_file = os.path.join(run_dir, f'{run_id}.log')
    file_handler = logging.FileHandler(log_file)
    file_handler.setLevel(logging.INFO)

    # Create console handler with a higher log level
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.ERROR)

    # Create formatter and add it to the handlers
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)

    # Add the handlers to the logger
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    return logger, file_handler


def main_workflow(dataset_key=None, num_topics=None, write_to_databse=False):
    # Determine if this is a standard run or an analysis run
    if dataset_key and num_topics:
        # Analysis mode
        filter_level = DATASETS[dataset_key]
        tm_params = {**COMMON_TM_PARAMS, 'num_topics': num_topics}
        run_dir_suffix = f"{dataset_key}_topics_{num_topics}"
        data_path = os.path.join(TOPIC_MODELING_DATA_DIR, f'{dataset_key}.json')  # Assuming each dataset is in a subfolder
    else:
        # Standard mode
        tm_params = TOPIC_MODELING_PARAMS
        run_dir_suffix = 'standard'
        data_path = os.path.join(TOPIC_MODELING_DATA_DIR, 'textual_features.json')  # The default data directory

    # Create run directory and setup logger
    run_id = datetime.now().strftime('%Y%m%d_%H%M%S')
    run_dir = create_topic_modeling_run_dir(f"{run_id}_{run_dir_suffix}")

    # Configure logger
    logger, file_handler = setup_logger(run_dir, run_id)

    # Log the topic modeling parameters
    logger.info("Starting topic modeling workflow...")
    logger.info(f"Run parameters: {tm_params}")
    topic_modeling.save_run_parameters(run_dir, tm_params)

    try:
        # Step 1: Get Textual Features
        start_time = time.time()  # Start timing
        logger.info("Step 1: Getting Textual Features")
        textual_features_df = topic_modeling.get_textual_features(data_path=data_path)
        end_time = time.time()  # End timing
        logger.info(
            f"Completed Step 1 in {format_duration(end_time - start_time)}.")

        # Step 2: Process and Concatenate Textual Features
        start_time = time.time()  # Start timing
        logger.info("Step 2: Processing and Concatenating Textual Features")
        processed_df = topic_modeling.process_and_concatenate_textual_features(
            textual_features_df)
        end_time = time.time()  # End timing
        logger.info(
            f"Completed Step 2 in {format_duration(end_time - start_time)}.")

        # Step 3: Create Dictionary and Corpus
        start_time = time.time()  # Start timing
        logger.info("Step 3: Creating Dictionary and Corpus")
        corpus, id2word, textual_features_df, filtered_df, remaining_tokens_ratio, filtered_out_tokens_ratio = topic_modeling.create_dictionary_and_corpus(df=processed_df,
                                                                                                                                                           min_token_frequency=tm_params[
                                                                                                                                                               'min_token_frequency'],
                                                                                                                                                           no_above=tm_params[
                                                                                                                                                               'no_above'],
                                                                                                                                                           min_tokens_per_document=tm_params['min_tokens_per_document'])
        end_time = time.time()  # End timing
        logger.info(
            f"Completed Step 3 in {format_duration(end_time - start_time)}.")

        # write textual features
        textual_features_df_path = os.path.join(
            run_dir, "textual_features_df.parquet")
        textual_features_df.to_parquet(
            textual_features_df_path, engine='pyarrow')

        # write filtering insights
        filtering_insights_path = os.path.join(
            run_dir, "filtering_insights.json")
        topic_modeling_file_management.write_insights_to_file(filtered_df, textual_features_df, remaining_tokens_ratio,
                                              filtered_out_tokens_ratio, tm_params, filtering_insights_path)

        # Step 4: Perform Topic Modelling
        start_time = time.time()  # Start timing
        logger.info("Step 4: Performing Topic Modeling")
        num_topics = tm_params['num_topics']
        model, topic_modeling_df = topic_modeling.perform_topic_modeling(
            filtered_df, corpus, id2word, num_topics)
        end_time = time.time()  # End timing
        logger.info(
            f"Completed Step 4 in {format_duration(end_time - start_time)}.")

        # Save a df of the video ids for which topic modeling was performed
        eligible_videos_path = os.path.join(run_dir, "eligible_videos.json")
        topic_modeling_df[['id']].to_json(
            eligible_videos_path, orient='records', lines=True)
        
        # Save topic distributions
        topic_modeling_file_management.save_topic_distributions(topic_modeling_df, run_dir)

        # Save topic information
        topic_modeling_file_management.save_topics_with_document_counts_and_plot(topic_modeling_df=topic_modeling_df, model=model, run_dir=run_dir, tm_params=tm_params)

        # Save word clouds
        topic_modeling_file_management.generate_and_save_wordclouds_from_df(topic_modeling_df=topic_modeling_df, model=model, run_dir=run_dir, num_topics=num_topics)

        # Save list of videos for each topic
        topic_modeling_file_management.save_topic_videos_overview(topic_modeling_df, run_dir, k=10)

        if write_to_databse:
            # Step 5: Write Topic Distributions to Database
            start_time = time.time()  # Start timing
            logger.info("Step 5: Writing Topic Distributions to Database")
            topic_modeling_file_management.update_topic_distributions_in_es(run_dir, run_id)
            end_time = time.time()  # End timing
            logger.info(
                f"Completed Step 5 in {format_duration(end_time - start_time)}.")

        logger.info("Workflow completed successfully.")

        # At the end of each workflow, remove handlers and close them
        handlers = logger.handlers[:]
        for handler in handlers:
            handler.close()
            logger.removeHandler(handler)


    except Exception as e:
        logger.error("An error occurred", exc_info=True)
        # Optionally, implement retry logic or save the current state for debugging


def standard_workflow():
    main_workflow()


def analysis_workflow():
    for dataset_key in DATASETS.keys():
        for num_topics in NUM_TOPICS_LIST:
            main_workflow(dataset_key, num_topics, write_to_databse=False)


if __name__ == "__main__":
    if args.analysis_mode:
        analysis_workflow()
    else:
        standard_workflow()
