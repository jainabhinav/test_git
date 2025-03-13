from prophecy.config import ConfigBase


class Config(ConfigBase):

    def __init__(self, workflow_name: str=None, **kwargs):
        self.spark = None
        self.update(workflow_name)

    def update(self, workflow_name: str="GSS_Savings_Integration_Model_Phase_2_WIP_V3", **kwargs):
        prophecy_spark = self.spark
        self.workflow_name = workflow_name
        pass
