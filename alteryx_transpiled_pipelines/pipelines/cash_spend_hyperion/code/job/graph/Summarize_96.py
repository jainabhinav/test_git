from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Summarize_96(spark: SparkSession, in0: DataFrame) -> DataFrame:
    df1 = in0.groupBy(
        col("`pa account desc`").alias("pa account desc"), 
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
        col("category"), 
        col("mc"), 
        col("hlmc"), 
        col("`regrouped level 4`").alias("regrouped level 4"), 
        col("`cost center`").alias("cost center"), 
        col("`product description`").alias("product description"), 
        col("`gl account number`").alias("gl account number")
    )

    return df1.agg(
        sum(col("Sum_USDAFX")).alias("Sum_USDAFX"), 
        sum(col("Sum_LCLCFX")).alias("Sum_LCLCFX"), 
        sum(col("Sum_LCL")).alias("Sum_LCL")
    )
