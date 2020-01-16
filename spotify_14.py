import sys
import json
import logging
import base64
import requests
import boto3
import pymysql
from datetime import datetime
import pandas as pd
import jsonpath


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
        conn = pymysql.connect(
            host=host,
            user=username,
            passwd=password,
            db=database,
            port=port,
            use_unicode=True,
            charset="utf8",
        )
        cursor = conn.cursor()
    except Exception:
        logging.error("could not connect to RDS")
        sys.exit(1)

    headers = get_headers(client_id, client_secret)

    cursor.execute("SELECT id FROM artists LIMIT 10")

    top_track_keys = {
        "id": "id",
        "name": "name",
        "popularity": "popularity",
        "external_url": "external_urls.spotify",
    }

    top_tracks = []

    for (id,) in cursor.fetchall():
        URL = "https://api.spotify.com/v1/artists/{}/top-tracks".format(id)
        params = {"country": "US"}
        r = requests.get(URL, params=params, headers=headers)
        raw = json.loads(r.text)

        for i in raw["tracks"]:
            top_track = {}
            for k, v in top_track_keys.items():
                top_track.update({k: jsonpath.jsonpath(i, v)})
                top_track.update({"artist_id": id})
                top_tracks.append(top_track)
                print("top-track append: ", top_track)

    track_ids = [i["id"][0] for i in top_tracks]

    top_tracks = pd.DataFrame(raw)
    top_tracks.to_parquet("top-tracks.parquet", engine="pyarrow", compressions="snappy")

    dt = datetime.utcnow().strftime("%Y-%m-%d")
    s3 = boto3.resource("s3")
    object = s3.Object(
        "feud72-spotify-artists", "top-tracks/dt={}/top-tracks.parquet".format(dt)
    )
    with open("top-tracks.parquet", "rb") as data:
        object.put(Body=data)
        print("put data: top-tracks.parquet")

    tracks_batch = [track_ids[i : i + 100] for i in range(0, len(track_ids), 100)]

    audio_features = []
    for i in tracks_batch:
        ids = ",".join(i)
        URL = "https://api.spotify.com/v1/audio-features/?ids={}".format(ids)

        r = requests.get(URL, headers=headers)
        raw = json.loads(r.text)
        audio_features.extend(raw["audio_features"])
        print("audio_features append")

    audio_features = pd.DataFrame(audio_features)
    audio_features.to_parquet(
        "audio-features.parquet", engine="pyarrow", compressions="snappy"
    )
    dt = datetime.utcnow().strftime("%Y-%m-%d")
    s3 = boto3.resource("s3")
    object = s3.Object(
        "feud72-spotify-artists",
        "audio-features/dt={}/audio-features.parquet".format(dt),
    )
    with open("audio-features.parquet", "rb") as data:
        object.put(Body=data)
        print("put data: top-tracks.parquet")


def get_headers(client_id, client_secret):

    endpoint = "https://accounts.spotify.com/api/token"
    encoded = base64.b64encode(f"{client_id}:{client_secret}".encode("utf-8")).decode(
        "ascii"
    )

    headers = {"Authorization": f"Basic {encoded}"}

    payload = {"grant_type": "client_credentials"}

    r = requests.post(endpoint, data=payload, headers=headers)

    access_token = json.loads(r.text)["access_token"]

    headers = {"Authorization": f"Bearer {access_token}"}

    return headers


if __name__ == "__main__":
    main()
