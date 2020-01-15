import sys
import os
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

    print("Success")


if __name__ == "__main__":
    main()
