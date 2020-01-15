import sys
import requests
import base64
import json
import logging
import time
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

    params = {"q": "BTS", "type": "artist", "limit": "5"}

    try:
        r = requests.get(
            "https://api.spotify.com/v1/search", params=params, headers=headers
        )
    except Exception:
        logging.error(r.text)
        sys.exit(1)

    raw = json.loads(r.text)
    #    print(raw["artists"]["items"][0].keys())
    artist_raw = raw["artists"]["items"][0]

    if artist_raw["name"] == params["q"]:
        artist = {
            "id": artist_raw["id"],
            "name": artist_raw["name"],
            "followers": artist_raw["followers"]["total"],
            "popularity": artist_raw["popularity"],
            "url": artist_raw["external_urls"]["spotify"],
            "image_url": artist_raw["images"][0]["url"],
        }

    query = """
    INSERT INTO artists (id, name, followers, popularity, url, image_url)
    VALUES ('{0}', '{1}', {2}, {3}, '{4}', '{5}')
    ON DUPLICATE KEY UPDATE id='{0}', name='{1}', followers={2}, popularity={3}, url='{4}', image_url='{5}'
    """.format(
        artist["id"],
        artist["name"],
        artist["followers"],
        artist["popularity"],
        artist["url"],
        artist["image_url"],
    )

    cursor.execute(query)
    conn.commit()

    sys.exit(0)

    if r.status_code != 200:
        logging.error(json.loads(r.text))
        if r.status_code == 429:
            retry_after = json.loads(r.headers)["Retry-After"]
            time.sleep(int(retry_after))
            r = requests.get(
                "https://api.spotify.com/v1/search", params=params, headers=headers
            )
        elif r.status_code == 401:
            headers = get_headers(client_id, client_secret)
            r = requests.get(
                "https://api.spotify.com/v1/search", params=params, headers=headers
            )
        else:
            sys.exit(1)

    # Get BTS' Albums

    r = requests.get(
        "https://api.spotify.com/v1/artists/3Nrfpe0tUJi4K4DXYWgMUX/albums",
        headers=headers,
    )

    raw = json.loads(r.text)

    # print(raw)

    next = raw["next"]

    albums = []
    albums.extend(raw["items"])

    # get 100

    while next and len(albums) < 100:
        r = requests.get(next, headers=headers)
        raw = json.loads(r.text)
        # print(raw)
        next = raw["next"]
        albums.extend(raw["items"])

    # print(len(albums))


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
