import boto3


class Glue:
    def __init__(self):
        self.client = boto3.client('glue')

    def start_job_run(self, job_name: str, arguments: dict = None) -> str:
        try:
            response = self.client.start_job_run(JobName=job_name, Arguments=arguments or {})
            return response['JobRunId']
        except Exception as e:
            raise RuntimeError(f"Failed to start Glue job '{job_name}': {e}")
