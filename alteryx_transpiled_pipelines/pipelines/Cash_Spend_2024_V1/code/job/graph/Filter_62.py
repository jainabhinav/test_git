from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Filter_62(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.filter(
        (
          (((((((((((((((((((((((((((((((((col("`tableau display mega category`") == lit("Agencies")).cast(BooleanType()) | (col("`tableau display mega category`") == lit("Biometrics")).cast(BooleanType())).cast(BooleanType()) | (col("`tableau display mega category`") == lit("Call Centers")).cast(BooleanType())).cast(BooleanType()) | (col("`tableau display mega category`") == lit("Construction")).cast(BooleanType())).cast(BooleanType()) | (col("`tableau display mega category`") == lit("Consulting")).cast(BooleanType())).cast(BooleanType()) | (col("`tableau display mega category`") == lit("Contract Manufacturing")).cast(BooleanType())).cast(BooleanType()) | (col("`tableau display mega category`") == lit("Devices")).cast(BooleanType())).cast(BooleanType()) | (col("`tableau display mega category`") == lit("Distribution")).cast(BooleanType())).cast(BooleanType()) | (col("`tableau display mega category`") == lit("Equipment")).cast(BooleanType())).cast(BooleanType()) | (col("`tableau display mega category`") == lit("Facility Services")).cast(BooleanType())).cast(BooleanType()) | (col("`tableau display mega category`") == lit("Financial Services")).cast(BooleanType())).cast(BooleanType()) | (col("`tableau display mega category`") == lit("Fleet")).cast(BooleanType())).cast(BooleanType()) | (col("`tableau display mega category`") == lit("IS Software, Hardware & Maintenance")).cast(BooleanType())).cast(BooleanType()) | (col("`tableau display mega category`") == lit("Lab Consumables & Support Services")).cast(BooleanType())).cast(BooleanType()) | (col("`tableau display mega category`") == lit("Legal Services")).cast(BooleanType())).cast(BooleanType()) | (col("`tableau display mega category`") == lit("Media")).cast(BooleanType())).cast(BooleanType()) | (col("`tableau display mega category`") == lit("Meetings")).cast(BooleanType())).cast(BooleanType()) | (col("`tableau display mega category`") == lit("Non Amgen Medicinal Products (NAMPS)")).cast(BooleanType())).cast(BooleanType()) | (col("`tableau display mega category`") == lit("Packaging & Containers")).cast(BooleanType())).cast(BooleanType()) | (col("`tableau display mega category`") == lit("Print & Fulfillment")).cast(BooleanType())).cast(BooleanType()) | (col("`tableau display mega category`") == lit("Raw Materials & Manufacturing Consumables")).cast(BooleanType())).cast(BooleanType()) | (col("`tableau display mega category`") == lit("Real Estate")).cast(BooleanType())).cast(BooleanType()) | (col("`tableau display mega category`") == lit("Research & Data")).cast(BooleanType())).cast(BooleanType()) | (col("`tableau display mega category`") == lit("Sales Support")).cast(BooleanType())).cast(BooleanType()) | (col("`tableau display mega category`") == lit("Scientific Services")).cast(BooleanType())).cast(BooleanType()) | (col("`tableau display mega category`") == lit("Site Pricing & Payments")).cast(BooleanType())).cast(BooleanType()) | (col("`tableau display mega category`") == lit("Staff Augmentation")).cast(BooleanType())).cast(BooleanType()) | (col("`tableau display mega category`") == lit("Staffing")).cast(BooleanType())).cast(BooleanType()) | (col("`tableau display mega category`") == lit("Technology Services")).cast(BooleanType())).cast(BooleanType()) | (col("`tableau display mega category`") == lit("Telecom")).cast(BooleanType())).cast(BooleanType()) | (col("`tableau display mega category`") == lit("Training")).cast(BooleanType())).cast(BooleanType()) | (col("`tableau display mega category`") == lit("Travel")).cast(BooleanType())).cast(BooleanType()) | (col("`tableau display mega category`") == lit("Trial Management")).cast(BooleanType())).cast(BooleanType())
          | (col("`tableau display mega category`") == lit("Utilities")).cast(BooleanType())
        )
    )
