import logging as pythonlogging
import os
import base64
import transcoderJob
from google.cloud import logging
from flask import Flask, request
from secure import require_apikey


app = Flask(__name__)


@app.route('/health')
def health():
    return 'It is alive!\n'


@app.route('/hello')
@require_apikey
def hello():
    return {'hello': 'world'}

@app.route("/", methods=["POST"])
def index():
    envelope = request.get_json()
    if not envelope:
        msg = "no Pub/Sub message received"
        print(f"error: {msg}")
        return f"Bad Request: {msg}", 400

    if not isinstance(envelope, dict) or "message" not in envelope:
        msg = "invalid Pub/Sub message format"
        print(f"error: {msg}")
        return f"Bad Request: {msg}", 400

    pubsub_message = envelope["message"]
    print(pubsub_message)
    
    client = logging.Client()

    logger = client.logger("service_1")
    logger.log(pubsub_message)
    
    name = "World"
    if isinstance(pubsub_message, dict) and "data" in pubsub_message:
        name = base64.b64decode(pubsub_message["data"]).decode("utf-8").strip()
    logger.log(name)
    print(f"Hello {name}!")
    inputs= name.split(",")

    transcoderJob.create_job_from_preset(inputs[0],inputs[1])

    return ("DONE", 204)

if __name__ == "__main__":
    app.run(debug=True, host='0.0.0.0', port=int(os.environ.get('PORT', 8080)))