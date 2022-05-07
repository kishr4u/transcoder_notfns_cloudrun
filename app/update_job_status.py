import argparse, sys, os, time, json
from google.cloud import logging, bigquery

from google.cloud import storage


def create_bucket(bucket_name):
    client = storage.Client()
    bucket = storage.Bucket(bucket_name)
    client.create_bucket(bucket)

def update_job_status_in_bq(message):
    
    parsed_json1 = (json.loads(message))
    client = logging.Client()

    logger = client.logger("service_1")
    logger.log("message: " + message)
    
    job_id=parsed_json1["job"]["name"]
    status=parsed_json1["job"]["state"]
    error=""
    if "error" in parsed_json1:
        error=parsed_json1["job"]["error"]["message"]


    logger.log("job: " + job_id + "," + status)
    print("job: " + job_id + "," + status)
    project_id="kishorerjbloom"
    dataset_id="test_sample"
    table_id="trancoder_job_dtls"
    
    
    client = bigquery.Client()
    query_text = f"""
    UPDATE `{project_id}.{dataset_id}.{table_id}`
    SET status = "{status}", error_msg = "{error}"
    WHERE job_id = "{job_id}"
    """
    query_job = client.query(query_text)

    # Wait for query job to finish.
    query_job.result()
    return 

if __name__ == "__main__":
    mesg = "{\"job\":{\"name\":\"projects/342382060728/locations/us-east1/jobs/6958b0bf-cb4c-446f-b765-5a9ba3fb7844\",\"state\":\"SUCCEEDED\"}}"
    update_job_status_in_bq(mesg)