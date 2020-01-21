import os
import sys
import json
import logging
import base64

sys.path.append("./libs")
import requests
import boto3

client_id = os.environ["client_id"]
client_secret = os.environ["client_secret"]

try:
    dynamodb = boto3.resource(
        "dynamodb",
        region_name="ap-northeast-2",
        endpoint_url="http://dynamodb.ap-northeast-2.amazonaws.com",
    )
except Exception:
    logging.error("could not connect to dynamodb")
    sys.exit(1)


def lambda_handler(event, context):
    headers = get_headers(client_id, client_secret)

    table = dynamodb.Table("top_tracks")
    artist_id = event["artist_id"]
    URL = "https://api.spotify.com/v1/artists/{}/top-tracks".format(artist_id)
    params = {"country": "US"}
    r = requests.get(URL, params=params, headers=headers)
    raw = json.loads(r.text)

    for track in raw["tracks"]:
        data = {"artist_id": artist_id}
        data.update(track)
        table.put_item(Item=data)

    return "SUCCESS"


def get_headers(client_id, client_secret):

    endpoint = "https://accounts.spotify.com/api/token"
    encoded = base64.b64encode(
        "{}:{}".format(client_id, client_secret).encode("utf-8")
    ).decode("ascii")

    headers = {"Authorization": "Basic {}".format(encoded)}

    payload = {"grant_type": "client_credentials"}

    r = requests.post(endpoint, data=payload, headers=headers)

    access_token = json.loads(r.text)["access_token"]

    headers = {"Authorization": "Bearer {}".format(access_token)}

    return headers
