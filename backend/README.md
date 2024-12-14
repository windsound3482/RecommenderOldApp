# Human-centric Recommender Systems Backend

This repository contains the backend implementation for the "Human-centric Recommender Systems: Implementing Models for User Influence and Explainability" master thesis project. It provides REST API endpoints to interact with different Recommender System (RS) models and facilitates database queries.

## Overview

The backend serves as a platform for making API calls to various RS models, with a focus on user influence and explainability within recommender systems.

## Getting Started

### Prerequisites

- Python 3.x
- Flask

### Running the Application

To run the Flask application:

1. Navigate to the root directory of the project.
2. Execute `flask run` in the terminal.

The application will be available at `http://127.0.0.1:5000`.

### Making API Requests

To obtain recommendations:

```bash
curl "<server_url>/recommendations?num=<number_of_recommendations>"
```

Replace {url} with the server URL and {number_of_recommendations} with the desired number of recommendations. The endpoint returns a list of video IDs randomly sampled from the dataset.

To perform the request in python:

```python
import requests

url = 'http://127.0.0.1:5000'

# Set the desired number of recommendations
number_of_recommendations = 10

# Perform an API call
response = requests.get(f'{url}/recommendations?num={number_of_recommendations}')

# Print response
print(response.json())
```

### Requirements

Dependencies are listed in requirements.txt, which captures the necessary packages for the project.
