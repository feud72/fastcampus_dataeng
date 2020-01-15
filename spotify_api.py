import sys
import requests
import base64
import json
import logging
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

    # Spotify Search API

    #    artists = []
    #
    #    with open("artist_list.csv") as f:
    #        raw = csv.reader(f)
    #        for row in raw:
    #            artists.append(row[0])
    #
    #    for a in artists:
    #        params = {"q": a, "type": "artist", "limit": "1"}
    #        r = requests.get(
    #            "https://api.spotify.com/v1/search", params=params, headers=headers
    #        )
    #        raw = json.loads(r.text)
    #        artist = {}
    #        try:
    #            artist_raw = raw["artists"]["items"][0]
    #            if artist_raw["name"] == params["q"]:
    #                artist.update(
    #                    {
    #                        "id": artist_raw["id"],
    #                        "name": artist_raw["name"],
    #                        "followers": artist_raw["followers"]["total"],
    #                        "popularity": artist_raw["popularity"],
    #                        "url": artist_raw["external_urls"]["spotify"],
    #                        "image_url": artist_raw["images"][0]["url"],
    #                    }
    #                )
    #                insert_row(cursor, artist, "artists")
    #                print("Done. id:", artist_raw["id"])
    #        except Exception:
    #            logging.error("NO ITEMS FROM SEARCH API")
    #            continue
    #
    #    conn.commit()

    cursor.execute("SELECT id FROM artists")
    artists = []
    for (id,) in cursor.fetchall():
        artists.append(id)

    artist_batch = [artists[i : i + 50] for i in range(0, len(artists), 50)]

    artist_genres = []

    for i in artist_batch:
        ids = ",".join(i)
        URL = "https://api.spotify.com/v1/artists/?ids={}".format(ids)

        r = requests.get(URL, headers=headers)
        raw = json.loads(r.text)

        for artist in raw["artists"]:
            for genre in artist["genres"]:
                artist_genres.append({"artist_id": artist["id"], "genre": genre})

    for data in artist_genres:
        insert_row(cursor, data, "artist_genres")

    conn.commit()
    sys.exit(0)

    #    if r.status_code != 200:
    #        logging.error(json.loads(r.text))
    #        if r.status_code == 429:
    #            retry_after = json.loads(r.headers)["Retry-After"]
    #            time.sleep(int(retry_after))
    #            r = requests.get(
    #                "https://api.spotify.com/v1/search", params=params, headers=headers
    #            )
    #        elif r.status_code == 401:
    #            headers = get_headers(client_id, client_secret)
    #            r = requests.get(
    #                "https://api.spotify.com/v1/search", params=params, headers=headers
    #            )
    #        else:
    #            sys.exit(1)

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


def insert_row(cursor, data, table):
    placeholders = ", ".join(["%s"] * len(data))
    columns = ", ".join(data.keys())
    key_placeholders = ", ".join(["{0}=%s".format(k) for k in data.keys()])
    sql = "INSERT INTO %s ( %s ) VALUES ( %s ) ON DUPLICATE KEY UPDATE %s" % (
        table,
        columns,
        placeholders,
        key_placeholders,
    )
    cursor.execute(sql, list(data.values()) * 2)


if __name__ == "__main__":
    main()
