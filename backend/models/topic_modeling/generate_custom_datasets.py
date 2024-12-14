import pandas as pd
import os

# Define the path to the original dataset
original_dataset_path = 'data/topic_modeling/textual_features.json'
output_directory = 'data/topic_modeling'

# Load the entire dataset
df = pd.read_json(original_dataset_path, lines=True)

# Define helper functions for language checks
def is_english(language):
    return str(language).startswith('en')

def is_european_language(language, languages=['en', 'es', 'fr', 'it', 'de']):
    return any(str(language).startswith(lang_code) for lang_code in languages)

def is_european_language_without_en(language, languages=['es', 'fr', 'it', 'de']):
    return any(str(language).startswith(lang_code) for lang_code in languages)

# Filtering functions for each layer
def filter_layer1(row):
    if not row['tags']:
        return False
    if is_english(row['defaultLanguage']) or is_english(row['defaultAudioLanguage']):
        return True
    return False

def filter_layer2(row):
    # Check that both 'defaultLanguage' and 'defaultAudioLanguage' are either missing or empty
    # And that 'predictedLanguage' is English
    if (not row.get('defaultLanguage') and not row.get('defaultAudioLanguage')) and (is_english(row['predictedLanguage'])):
        return True
    return False


def filter_layer3(row):
    if not row['tags']:
        return False
    if is_european_language_without_en(row['defaultLanguage']) or is_european_language_without_en(row['defaultAudioLanguage']) or is_european_language_without_en(row['predictedLanguage']):
        return True
    return False

# Define the layers
layer1 = df[df.apply(filter_layer1, axis=1)]
layer2 = df[df.apply(filter_layer2, axis=1)]
layer3 = df[df.apply(filter_layer3, axis=1)]

# Define the aggregated datasets
dataset1 = df[df.apply(filter_layer1, axis=1)]
dataset2 = df[df.apply(filter_layer1, axis=1) | df.apply(filter_layer2, axis=1)]
dataset3 = df[df.apply(filter_layer1, axis=1) | df.apply(filter_layer2, axis=1) | df.apply(filter_layer3, axis=1)]

# Save all resulting datasets
layer1.to_json(os.path.join(output_directory, 'layer1.json'), orient='records', lines=True)
layer2.to_json(os.path.join(output_directory, 'layer2.json'), orient='records', lines=True)
layer3.to_json(os.path.join(output_directory, 'layer3.json'), orient='records', lines=True)
dataset1.to_json(os.path.join(output_directory, 'dataset1.json'), orient='records', lines=True)
dataset2.to_json(os.path.join(output_directory, 'dataset2.json'), orient='records', lines=True)
dataset3.to_json(os.path.join(output_directory, 'dataset3.json'), orient='records', lines=True)

# Get length of entire dataset
dataset_size = len(df)

# Calculate indices of layers and datasets
layer1_indices = set(layer1.index)
layer2_indices = set(layer2.index)
layer3_indices = set(layer3.index)
dataset1_indices = set(dataset1.index)
dataset2_indices = set(dataset2.index)
dataset3_indices = set(dataset3.index)

# Calculating sizes
size_layer1 = len(layer1_indices)
size_layer2 = len(layer2_indices)
size_layer3 = len(layer3_indices)
size_dataset1 = len(dataset1_indices)
size_dataset2 = len(dataset2_indices)
size_dataset3 = len(dataset3_indices)

# Calculating layer overlaps
overlap_layer1_layer2 = len(layer1_indices & layer2_indices)
overlap_layer1_layer3 = len(layer1_indices & layer3_indices)
overlap_layer2_layer3 = len(layer2_indices & layer3_indices)

# Calculating dataset overlaps
overlap_dataset1_dataset2 = len(dataset1_indices & dataset2_indices)
overlap_dataset1_dataset3 = len(dataset1_indices & dataset3_indices)
overlap_dataset2_dataset3 = len(dataset2_indices & dataset3_indices)

# Printing sizes layers
print(f"Size of Layer 1: {size_layer1}")
print(f"Size of Layer 2: {size_layer2}")
print(f"Size of Layer 3: {size_layer3}")
print(f"Size of dataset 1: {size_dataset1}")
print(f"Size of dataset 2: {size_dataset2}")
print(f"Size of dataset 3: {size_dataset3}\n")

# Printing sizes layers
print(f"Dataset ratio of Layer 1: {round(100 * size_layer1/dataset_size)}%")
print(f"Dataset ratio of Layer 2: {round(100 * size_layer2/dataset_size)}%")
print(f"Dataset ratio of Layer 3: {round(100 * size_layer3/dataset_size)}%")
print(f"Dataset ratio of dataset 1: {round(100 * size_dataset1/dataset_size)}%")
print(f"Dataset ratio of dataset 2: {round(100 * size_dataset2/dataset_size)}%")
print(f"Dataset ratio of dataset 3: {round(100 * size_dataset3/dataset_size)}%\n")

# Printing overlaps
print(f"Overlap between Layer 1 and Layer 2: {overlap_layer1_layer2}")
print(f"Overlap between Layer 1 and Layer 3: {overlap_layer1_layer3}")
print(f"Overlap between Layer 2 and Layer 3: {overlap_layer2_layer3}")
print(f"Overlap between dataset 1 and dataset 2: {overlap_dataset1_dataset2}")
print(f"Overlap between dataset 1 and dataset 3: {overlap_dataset1_dataset3}")
print(f"Overlap between dataset 2 and dataset 3: {overlap_dataset1_dataset2}")
