from src.adapter.aws.aws import AWS
from src.config.constants import GLUE_BDI_JOB_NAME
from src.util.utils import Utils


class LambdaFunction:
    def __init__(self):
        self._aws = AWS()
        self._glue = self._aws.glue()
        self._utils = Utils()

    def handle(self, event, context):
        self._process_event(event)

    def _process_event(self, event):
        try:
            bucket_name = self._utils.extract_s3_bucket_name(event)
            s3_key = self._utils.extract_s3_key(event)
            print(f"Processing S3 event for bucket: {bucket_name}, key: {s3_key}")

            arguments = {
                '--S3_INPUT_FILE': f's3://{bucket_name}/{s3_key}',
            }

            job_run_id = self._glue.start_job_run(GLUE_BDI_JOB_NAME, arguments)
            print(f"Started Glue job '{GLUE_BDI_JOB_NAME}' with run ID: {job_run_id}")
        except Exception as e:
            raise RuntimeError(f"Failed to process event: {e}")


lambda_handle = LambdaFunction().handle


def lambda_function(event, context):
    print("Received event:", event)
    lambda_handle(event, context)
