from prophecy.config import ConfigBase


class Config(ConfigBase):

    def __init__(self, workflow_name: str=None, **kwargs):
        self.spark = None
        self.update(workflow_name)

    def update(self, workflow_name: str="", **kwargs):
        prophecy_spark = self.spark
        self.workflow_name = workflow_name
        pass
