import sys
import requests
import base64
import json
import logging
import time

json_data = open("./api_key.json").read()

client_data = json.loads(json_data)

client_id = client_data["client_id"]
client_secret = client_data["client_secret"]


def main():

    headers = get_headers(client_id, client_secret)

    params = {"q": "BTS", "type": "artist", "limit": "5"}

    try:
        r = requests.get(
            "https://api.spotify.com/v1/search", params=params, headers=headers
        )
    except Exception:
        logging.error(r.text)
        sys.exit(1)

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
