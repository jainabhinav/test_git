from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def AlteryxSelect_420(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("mc"), 
        col("`gl account number`").alias("gl account number"), 
        col("Site"), 
        col("`pa account`").alias("pa account"), 
        col("`scenario name`").alias("scenario name"), 
        col("hlmc"), 
        col("`Regrouped Level 4`").alias("Regrouped Level 4"), 
        col("year"), 
        col("evp"), 
        col("`cost center number`").alias("cost center number"), 
        col("`cost center`").alias("cost center"), 
        col("`Exclusion Type`").alias("Exclusion Type"), 
        col("`pa account desc`").alias("pa account desc"), 
        col("`sub mc`").alias("sub mc"), 
        col("`material group`").alias("material group"), 
        col("quarter"), 
        col("category"), 
        col("`material group description`").alias("material group description"), 
        col("Sum_Adjusted_Sum_LCLCFX").alias("Sum_LCLCFX"), 
        col("`company code number`").alias("company code"), 
        col("`company description`").alias("company"), 
        col("`GL Account Description Updated`").alias("gl account")
    )
