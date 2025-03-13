from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Summarize_403(spark: SparkSession, in0: DataFrame) -> DataFrame:
    df1 = in0.groupBy(
        col("`company code number`").alias("company code number"), 
        col("`source_cost center`").alias("cost center"), 
        col("`source_accounting date - quarter`").alias("quarter"), 
        col("`source_mg code description`").alias("material group description"), 
        col("GL").alias("gl account number"), 
        col("`GL Account Description Updated`").alias("GL Account Description Updated"), 
        col("`hyperion sub mc`").alias("sub mc"), 
        col("`tableau display mega category`").alias("category"), 
        col("`hyperion evp`").alias("evp"), 
        col("`Planning Account`").alias("pa account"), 
        col("`source_cost center number`").alias("cost center number"), 
        col("`Regrouped Level 4`").alias("Regrouped Level 4"), 
        col("Site"), 
        col("`source_mg code`").alias("material group"), 
        col("`source_accounting date - year`").alias("year"), 
        col("`Exclusion Type`").alias("Exclusion Type"), 
        col("`hyperion hlmc`").alias("hlmc"), 
        col("`company code name`").alias("company description"), 
        col("`hyperion mc`").alias("mc")
    )

    return df1.agg(sum(col("`Spend ($)`")).alias("Sum_Adjusted_Sum_LCLCFX"))
