from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Union_181_reformat_1(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        lit(None).cast(StringType()).alias("CTS v3"), 
        col("Category"), 
        col("Sum_Adjusted_Sum_LCL"), 
        col("Sum_Adjusted_Sum_LCLCFX"), 
        col("Sum_Adjusted_Sum_USDAFX"), 
        col("company"), 
        col("`company code`").alias("company code"), 
        col("`cost center`").alias("cost center"), 
        col("`cost center number`").alias("cost center number"), 
        col("evp"), 
        col("`gl account`").alias("gl account"), 
        col("`gl account number`").alias("gl account number"), 
        col("hlmc"), 
        col("`hyperion category`").alias("hyperion category"), 
        col("`hyperion category code`").alias("hyperion category code"), 
        col("`material group`").alias("material group"), 
        col("`material group description`").alias("material group description"), 
        col("mc"), 
        col("`pa account`").alias("pa account"), 
        col("`pa account desc`").alias("pa account desc"), 
        col("`planning sku`").alias("planning sku"), 
        col("`product description`").alias("product description"), 
        col("quarter"), 
        col("`regrouped level 4`").alias("regrouped level 4"), 
        col("`scenario name`").alias("scenario name"), 
        col("`scenario type`").alias("scenario type"), 
        col("site"), 
        col("`sub mc`").alias("sub mc"), 
        col("version"), 
        col("year")
    )
