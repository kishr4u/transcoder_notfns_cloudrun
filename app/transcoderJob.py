
import argparse, sys, os
from google.cloud.video import transcoder_v1
from google.cloud.video.transcoder_v1.services.transcoder_service import (
    TranscoderServiceClient,
)
from google.cloud import storage


def create_bucket(bucket_name):
    client = storage.Client()
    bucket = storage.Bucket(bucket_name)
    client.create_bucket(bucket)

def create_job_from_preset( input_bucket_uri, input_object):
    """Creates a job based on a job preset.

    Args:
        project_id: The GCP project ID.
        location: The location to start the job in.
        input_uri: Uri of the video in the Cloud Storage bucket.
        output_uri: Uri of the video output folder in the Cloud Storage bucket.
        preset: The preset template (for example, 'preset/web-hd')."""

    client = TranscoderServiceClient()

    input_uri=input_bucket_uri+input_object
    output_uri="gsutil://media-out-kish"+input_bucket_uri+"/"+ input_object

    project_id = os.environ.get('project_id')
    location = os.environ.get('location')

    parent = "projects/kishorerjbloom/locations/us-east1"
    job = transcoder_v1.types.Job()
    job.input_uri = input_uri
    job.output_uri = output_uri
    job.template_id = "TEMP6"
    job.

    response = client.create_job(parent=parent, job=job)
    print(f"Job: {response.name}")
    return response


if( __name__ == "__main__"):
    create_job_from_preset(sys.argv[1],sys.argv[2])