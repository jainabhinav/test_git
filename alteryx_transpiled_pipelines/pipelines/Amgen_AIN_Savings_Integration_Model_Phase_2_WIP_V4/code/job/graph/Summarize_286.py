from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Summarize_286(spark: SparkSession, in0: DataFrame) -> DataFrame:
    df1 = in0.groupBy(
        col("`pa account desc`").alias("pa account desc"), 
        col("ASHB"), 
        col("Category"), 
        col("`scenario name`").alias("scenario name"), 
        col("`company code`").alias("company code"), 
        col("`sub mc`").alias("sub mc"), 
        col("site"), 
        col("`material group`").alias("material group"), 
        col("`OSE Labor`").alias("OSE Labor"), 
        col("quarter"), 
        col("Year"), 
        col("`hyperion category code`").alias("hyperion category code"), 
        col("company"), 
        col("version"), 
        col("`scenario type`").alias("scenario type"), 
        col("`planning sku`").alias("planning sku"), 
        col("`cost center number`").alias("cost center number"), 
        col("`gl account`").alias("gl account"), 
        col("`hyperion category`").alias("hyperion category"), 
        col("evp"), 
        col("`pa account`").alias("pa account"), 
        col("`material group description`").alias("material group description"), 
        col("mc"), 
        col("hlmc"), 
        col("`regrouped level 4`").alias("regrouped level 4"), 
        col("`CTS v3`").alias("CTS v3"), 
        col("`cost center`").alias("cost center"), 
        col("`product description`").alias("product description"), 
        col("`gl account number`").alias("gl account number")
    )

    return df1.agg(
        sum(col("AIN_Resource_Cost")).alias("AIN_Resource_Cost"), 
        sum(col("AIN_Savings")).alias("AIN_Savings"), 
        sum(col("EW_Reduction")).alias("EW_Reduction")
    )
