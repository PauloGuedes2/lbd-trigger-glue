from lambda_function import lambda_handler


def simulate_event():
    return {
  "Records": [
    {
      "eventVersion": "2.1",
      "eventSource": "aws:s3",
      "awsRegion": "us-east-1",
      "eventTime": "2025-07-08T00:10:54.218Z",
      "eventName": "ObjectCreated:Put",
      "userIdentity": {
        "principalId": "AWS:AROAXDZ77ICPHDLDJ7CZO:lbd-scrapper"
      },
      "requestParameters": {
        "sourceIPAddress": "18.234.187.34"
      },
      "responseElements": {
        "x-amz-request-id": "T7JCK4PM05KMP67N",
        "x-amz-id-2": "BPykOdRWs2VEF1cJybhHXPXIBW6EBslAvkvuYftE3P70YBartJMaVvTHSmMsvnC1sOvRwma7DIPvzDhnFCUHuE9IzWPwMD2S"
      },
      "s3": {
        "s3SchemaVersion": "1.0",
        "configurationId": "1c951347-98d2-46fe-9118-1d281332aedf",
        "bucket": {
          "name": "raw-buckets3-bovespa",
          "ownerIdentity": {
            "principalId": "A3MABZ64EPXUYO"
          },
          "arn": "arn:aws:s3:::raw-buckets3-bovespa"
        },
        "object": {
          "key": "raw/anomesdia%3D20250708/ibov.parquet",
          "size": 5204,
          "eTag": "8ecc72cdf88266bc155de7fd6960a330",
          "sequencer": "00686C620E2BFBA6E4"
        }
      }
    }
  ]
}


if __name__ == "__main__":
    lambda_handler(simulate_event(), {})