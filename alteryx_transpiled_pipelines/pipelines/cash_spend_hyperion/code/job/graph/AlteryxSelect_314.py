from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def AlteryxSelect_314(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("`pa account desc`").alias("pa account desc"), 
        col("`scenario name`").alias("scenario name"), 
        col("`company code`").alias("company code"), 
        col("`sub mc`").alias("sub mc"), 
        col("site"), 
        col("`Non-Opex Adjustments`").alias("Non-Opex Adjustments"), 
        col("year"), 
        col("`material group`").alias("material group"), 
        col("quarter"), 
        col("`hyperion category code`").alias("hyperion category code"), 
        col("company"), 
        col("version"), 
        col("`scenario type`").alias("scenario type"), 
        col("`planning sku`").alias("planning sku"), 
        col("Sum_Adjusted_Sum_USDAFX"), 
        col("`cost center number`").alias("cost center number"), 
        col("`gl account`").alias("gl account"), 
        col("`% Split`").alias("% Split"), 
        col("`hyperion category`").alias("hyperion category"), 
        col("evp"), 
        col("`pa account`").alias("pa account"), 
        col("Sum_Adjusted_Sum_LCL"), 
        col("Category"), 
        col("mc"), 
        col("hlmc"), 
        col("`regrouped level 4`").alias("regrouped level 4"), 
        col("`cost center`").alias("cost center"), 
        col("`product description`").alias("product description"), 
        col("Sum_Adjusted_Sum_LCLCFX"), 
        col("`gl account number`").alias("gl account number")
    )
