from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def AlteryxSelect_91(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("mc"), 
        col("`planning sku`").alias("planning sku"), 
        col("company"), 
        col("`gl account number`").alias("gl account number"), 
        col("`regrouped level 4`").alias("regrouped level 4"), 
        col("Allocated_Sum_LCL"), 
        col("`pa account`").alias("pa account"), 
        col("`product description`").alias("product description"), 
        col("Allocated_Sum_LCLCFX"), 
        col("`scenario name`").alias("scenario name"), 
        col("hlmc"), 
        col("`scenario type`").alias("scenario type"), 
        col("version"), 
        col("year"), 
        col("`gl account`").alias("gl account"), 
        col("evp"), 
        col("`cost center number`").alias("cost center number"), 
        col("`cost center`").alias("cost center"), 
        col("`pa account desc`").alias("pa account desc"), 
        col("`sub mc`").alias("sub mc"), 
        col("`material group`").alias("material group"), 
        col("site"), 
        col("`Data Source`").alias("Data Source"), 
        col("quarter"), 
        col("Allocated_Sum_USDAFX"), 
        col("category").alias("hyperion category"), 
        col("`category code`").alias("hyperion category code"), 
        col("`company code`").cast(StringType()).alias("company code")
    )
