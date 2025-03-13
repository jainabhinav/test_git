from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def AlteryxSelect_427(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("mc"), 
        col("`planning sku`").alias("planning sku"), 
        col("company"), 
        col("`gl account number`").alias("gl account number"), 
        col("Sum_USDAFX"), 
        col("`regrouped level 4`").alias("regrouped level 4"), 
        col("`pa account`").alias("pa account"), 
        col("`company code`").alias("company code"), 
        col("`product description`").alias("product description"), 
        col("`scenario name`").alias("scenario name"), 
        col("hlmc"), 
        col("`scenario type`").alias("scenario type"), 
        col("version"), 
        col("Sum_LCLCFX"), 
        col("year"), 
        col("`gl account`").alias("gl account"), 
        col("evp"), 
        col("`cost center number`").alias("cost center number"), 
        col("`cost center`").alias("cost center"), 
        col("`Exclusion Type`").alias("Exclusion Type"), 
        col("`pa account desc`").alias("pa account desc"), 
        col("`sub mc`").alias("sub mc"), 
        col("`material group`").alias("material group"), 
        col("site"), 
        col("`category code`").alias("category code"), 
        col("quarter"), 
        col("Sum_LCL"), 
        col("category")
    )
