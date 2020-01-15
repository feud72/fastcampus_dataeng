import sys
import logging
import boto3


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

    response = table.get_item(
        Key={"artist_id": "00FQb4jTyendYWaN8pK0wa", "id": "0Oqc0kKFsQ6MhFOLBNZIGX"}
    )

    print(response)


if __name__ == "__main__":
    main()
