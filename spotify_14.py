import sys
import json
import logging
import base64
import requests
import boto3
import pymysql
from datetime import datetime

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

    cursor.execute("SELECT id FROM artists")

    dt = datetime.utcnow().strftime("%Y-%m-%d")
    print(dt)
    sys.exit(0)

    for (id,) in cursor.fetchall():
        pass

    with open("top_tracks.json", "w") as f:
        for i in top_tracks:
            json.dump(i, f)
            f.write(os.linesep)

    s3 = boto3.resource("s3")
    object = s3.Object("spotify-artists", "dt={}/top-tracks.json".format(dt))


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
