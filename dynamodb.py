import sys
import json
import logging
import base64
import requests
import boto3
import pymysql


secret_data = open("./secret.json").read()

client_data = json.loads(secret_data)["client_data"]
db_data = json.loads(secret_data)["db_data"]

client_id = client_data["client_id"]
client_secret = client_data["client_secret"]

host = db_data["host"]
port = db_data["port"]
username = db_data["username"]
database = db_data["database"]
password = db_data["password"]


def main():
    try:
        dynamodb = boto3.resource(
            "dynamodb",
            region_name="ap-northeast-2",
            endpoint_url="http://dynamodb.ap-northeast-2.amazonaws.com",
        )
    except Exception:
        logging.error("could not connect to dynamodb")
        sys.exit(1)

    try:
        conn = pymysql.connect(
            host,
            user=username,
            passwd=password,
            db=database,
            port=port,
            use_unicode=True,
            charset="utf8",
        )
        cursor = conn.cursor()
    except Exception:
        logging.error("could not connect to rds")
        sys.exit(1)

    headers = get_headers(client_id, client_secret)

    table = dynamodb.Table("top_tracks")

    cursor.execute("SELECT id FROM artists")

    for (artist_id,) in cursor.fetchall():

        URL = "https://api.spotify.com/v1/artists/{}/top-tracks".format(artist_id)
        params = {"country": "US"}
        r = requests.get(URL, params=params, headers=headers)
        raw = json.loads(r.text)

        for track in raw["tracks"]:
            data = {"artist_id": artist_id}
            data.update(track)
            table.put_item(Item=data)


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


if __name__ == "__main__":
    main()
