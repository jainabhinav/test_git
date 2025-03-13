from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Formula_246(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("`pa account desc`").alias("pa account desc"), 
        col("ASHB"), 
        col("Category"), 
        col("`scenario name`").alias("scenario name"), 
        col("`company code`").alias("company code"), 
        col("`sub mc`").alias("sub mc"), 
        col("site"), 
        col("year"), 
        col("`material group`").alias("material group"), 
        col("`OSE Labor`").alias("OSE Labor"), 
        col("quarter"), 
        col("Sum_Sum_Adjusted_Sum_LCLCFX"), 
        col("`hyperion category code`").alias("hyperion category code"), 
        col("company"), 
        col("version"), 
        col("`scenario type`").alias("scenario type"), 
        col("Group"), 
        col("`planning sku`").alias("planning sku"), 
        col("`cost center number`").alias("cost center number"), 
        col("`gl account`").alias("gl account"), 
        col("`hyperion category`").alias("hyperion category"), 
        col("evp"), 
        col("`Split Divide`").alias("Split Divide"), 
        col("Sum_Sum_Sum_Adjusted_Sum_LCLCFX"), 
        col("`pa account`").alias("pa account"), 
        col("`material group description`").alias("material group description"), 
        col("mc"), 
        col("hlmc"), 
        col("`regrouped level 4`").alias("regrouped level 4"), 
        col("`CTS v3`").alias("CTS v3"), 
        col("`cost center`").alias("cost center"), 
        col("`product description`").alias("product description"), 
        col("`Right_scenario name`").alias("Right_scenario name"), 
        col("`gl account number`").alias("gl account number"), 
        (col("Sum_Sum_Adjusted_Sum_LCLCFX") / col("Sum_Sum_Sum_Adjusted_Sum_LCLCFX"))\
          .cast(DoubleType())\
          .alias("DTI % Split")
    )
