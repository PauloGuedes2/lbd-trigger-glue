from adapter.aws.aws import AWS
from config.constants import GLUE_BDI_JOB_NAME
from util.utils import Utils


class LambdaFunction:
    def __init__(self):
        self._aws = AWS()
        self._glue = self._aws.glue
        self._utils = Utils()

    def handle(self, event, context):
        self._process_event(event)

    def _process_event(self, event):
        glue_arguments = {
            "--enable-metrics": "true",
            "--enable-spark-ui": "true",
            "--spark-event-logs-path": "s3://aws-glue-assets-489223569566-us-east-1/sparkHistoryLogs/",
            "--enable-job-insights": "true",
            "--enable-glue-datacatalog": "true",
            "--job-bookmark-option": "job-bookmark-disable",
            "--job-language": "python",
            "--TempDir": "s3://aws-glue-assets-489223569566-us-east-1/temporary/",
            "--enable-auto-scaling": "true"
        }

        try:
            bucket_name = self._utils.extract_s3_bucket_name(event)
            s3_key = self._utils.extract_s3_key(event)
            print(f"Processing S3 event for bucket: {bucket_name}, key: {s3_key}")

            job_run_id = self._glue.start_job_run(GLUE_BDI_JOB_NAME, glue_arguments)
            print(f"Started Glue job '{GLUE_BDI_JOB_NAME}' with run ID: {job_run_id}")
        except Exception as e:
            raise RuntimeError(f"Failed to process event: {e}")


lambda_handle = LambdaFunction().handle


def lambda_handler(event, context):
    print("Received event:", event)
    lambda_handle(event, context)
