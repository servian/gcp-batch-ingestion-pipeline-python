# gcp-batch-ingestion-pipeline-python
A Python application that builds a conga line of GCS -> Cloud Functions -> Dataflow (template) -> BigQuery. It consists of two projects: a Cloud Function writted in Node.js and a Dataflow job written with the Python SDK.

## Cloud Functions

A Cloud Function is a single purpose Node.js function hosted on GCP that respond to events on GCP. In this case, we are using a cloud function to trigger a Dataflow template based on a file placed in Google Cloud Storage.

### Requirements

Running the project requires the Google Cloud SDK to be installed and authenticated to your project.

### Building your Cloud Function

In this project, we are transpiling our Node.js code with babel to ensure compatibility with Google Cloud. To build, run the following:

    npm run build

### Deploying your Cloud Function

Install npm modules (npm install) and deploy with the following command:

    npm run deploy

This script runs the 'gcloud functions deploy' command    

## Dataflow 
