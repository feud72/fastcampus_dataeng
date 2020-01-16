import sys
import logging
import pprint

import boto3
from boto3.dynamodb.conditions import Key, Attr


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

    table = dynamodb.Table("top_tracks")

    response = table.query(
        KeyConditionExpression=Key("artist_id").eq("00FQb4jTyendYWaN8pK0wa"),
        FilterExpression=Attr("popularity").gt(80),
    )

    pprint.pp(response["Items"])


if __name__ == "__main__":
    main()
