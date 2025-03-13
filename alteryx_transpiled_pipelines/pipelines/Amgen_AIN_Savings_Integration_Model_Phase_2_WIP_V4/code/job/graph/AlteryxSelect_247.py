from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def AlteryxSelect_247(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("mc"), 
        col("`OSE Labor`").alias("OSE Labor"), 
        col("`planning sku`").alias("planning sku"), 
        col("`Split Divide`").alias("Split Divide"), 
        col("ASHB"), 
        col("`hyperion category code`").alias("hyperion category code"), 
        col("company"), 
        col("`gl account number`").alias("gl account number"), 
        col("Sum_Sum_Adjusted_Sum_LCLCFX"), 
        col("`regrouped level 4`").alias("regrouped level 4"), 
        col("`pa account`").alias("pa account"), 
        col("`company code`").alias("company code"), 
        col("`product description`").alias("product description"), 
        col("`scenario name`").alias("scenario name"), 
        col("hlmc"), 
        col("`scenario type`").alias("scenario type"), 
        col("version"), 
        col("year"), 
        col("`gl account`").alias("gl account"), 
        col("evp"), 
        col("`hyperion category`").alias("hyperion category"), 
        col("`cost center number`").alias("cost center number"), 
        col("`CTS v3`").alias("CTS v3"), 
        col("`cost center`").alias("cost center"), 
        col("`pa account desc`").alias("pa account desc"), 
        col("`sub mc`").alias("sub mc"), 
        col("`material group`").alias("material group"), 
        col("Category"), 
        col("site"), 
        col("quarter"), 
        col("`material group description`").alias("material group description")
    )
