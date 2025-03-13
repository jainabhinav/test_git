from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Formula_417(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("`pa account desc`").alias("pa account desc"), 
        col("Sum_LCL"), 
        col("`scenario name`").alias("scenario name"), 
        col("`company code`").alias("company code"), 
        col("`sub mc`").alias("sub mc"), 
        col("site"), 
        col("year"), 
        col("`category code`").alias("category code"), 
        col("`material group`").alias("material group"), 
        col("quarter"), 
        col("company"), 
        col("version"), 
        col("`scenario type`").alias("scenario type"), 
        col("`planning sku`").alias("planning sku"), 
        col("`cost center number`").alias("cost center number"), 
        col("`gl account`").alias("gl account"), 
        col("evp"), 
        col("`pa account`").alias("pa account"), 
        col("Sum_LCLCFX"), 
        col("category"), 
        col("mc"), 
        col("hlmc"), 
        col("`regrouped level 4`").alias("regrouped level 4"), 
        col("`cost center`").alias("cost center"), 
        col("`product description`").alias("product description"), 
        col("Sum_USDAFX"), 
        col("`gl account number`").alias("gl account number"), 
        lit("Non-Addressable PA Accounts").cast(StringType()).alias("Exclusion Type")
    )
