from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Union_257_reformat_0(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("Flag"), 
        col("Sum_LCL"), 
        col("Sum_LCLCFX"), 
        col("Sum_USDAFX"), 
        col("category"), 
        col("`category code`").alias("category code"), 
        col("company"), 
        col("`company code`").alias("company code"), 
        col("`cost center`").alias("cost center"), 
        col("`cost center number`").alias("cost center number"), 
        col("evp"), 
        col("`gl account`").alias("gl account"), 
        col("`gl account number`").alias("gl account number"), 
        col("hlmc"), 
        col("`material group`").alias("material group"), 
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
