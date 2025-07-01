from lambda_function import lambda_handler


def simulate_event():
    return {
        "Records": [
            {
                "eventVersion": "2.1",
                "eventSource": "aws:s3",
                "awsRegion": "us-east-1",
                "eventTime": "2024-06-13T12:00:00.000Z",
                "eventName": "ObjectCreated:Put",
                "s3": {
                    "bucket": {
                        "name": "bucket-bovespadata"
                    },
                    "object": {
                        "key": "rawdata/20250613.parquet",
                        "size": 12345
                    }
                }
            }
        ]
    }


if __name__ == "__main__":
    lambda_handler(simulate_event, {})