# gcp-batch-ingestion-pipeline-python
A Python application that builds a conga line of GCS -> Cloud Functions -> Dataflow (template) -> BigQuery. It consists of two projects: a Cloud Function writted in Node.js and a Dataflow job written with the Python SDK.

When deploying, be sure that you are authenticated with GCP. https://cloud.google.com/sdk/docs/initializing

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

The templated Dataflow pipeline gets invoked via a Cloud Function trigger listning on a particular GCS bucket. The cloud function passes the fully qualified path of the file to the templated dataflow located in GCS. 

The Dataflow pipeline reads the newly landed file and loads into BigQuery. Also as a side output, the meta data about the job execution is written out to Cloud DataStore DB.

Note: By commenting out the --templete_location pipeline argument, the dataflow pipeline can be invoked directly.
