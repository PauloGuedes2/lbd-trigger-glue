import json
import urllib.parse


class Utils:
    @staticmethod
    def decode_s3_key(key: str) -> str:
        return urllib.parse.unquote_plus(key)

    @staticmethod
    def extract_s3_bucket_name(event: json) -> json:
        try:
            for record in event['Records']:
                return record['s3']['bucket']['name']
        except Exception as e:
            raise ValueError(f"Failed to extract S3 bucket name: {e}")

    @staticmethod
    def extract_s3_key(event: json) -> json:
        try:
            for record in event['Records']:
                return Utils().decode_s3_key(record['s3']['object']['key'])
        except Exception as e:
            raise ValueError(f"Failed to extract S3 key: {e}")
