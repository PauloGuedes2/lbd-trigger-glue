from adapter.aws.glue.glue import Glue


class AWS:
    def __init__(self):
        try:
            self.glue = Glue()
        except Exception as e:
            raise Exception(f"Failed to initialize AWS services: {e}")