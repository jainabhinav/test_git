from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Union_29_reformat_1(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("`_CC Lookup`").alias("_CC Lookup"), 
        col("`_CTS v3`").alias("_CTS v3"), 
        col("`_CTS v3 CC`").alias("_CTS v3 CC"), 
        col("_Category"), 
        col("`_Category Old`").alias("_Category Old"), 
        col("_Description").cast(StringType()).alias("_Description"), 
        col("_GL"), 
        col("`_GL Number`").alias("_GL Number"), 
        col("`_Gl Account`").alias("_Gl Account"), 
        col(
            "`_Gl Account (# only)`"
          )\
          .alias(
          "_Gl Account (# only)"
        ), 
        col("`_Mega Category Old`").alias("_Mega Category Old"), 
        col("_Name").cast(StringType()).alias("_Name"), 
        col("`_OSE Labor`").alias("_OSE Labor"), 
        col(
          "_PA#"
        ), 
        col("`_Planning Account`").alias("_Planning Account"), 
        col("`_Regrouped Level 4`").alias("_Regrouped Level 4"), 
        col("_Right_GL").cast(StringType()).alias("_Right_GL"), 
        col("`_Sum_Sum_Spend ($)`").alias("_Sum_Sum_Spend ($)"), 
        col("`_company code number`").alias("_company code number"), 
        col("`_hyperion evp`").alias("_hyperion evp"), 
        col("`_hyperion hlmc`").alias("_hyperion hlmc"), 
        col("`_hyperion mc`").alias("_hyperion mc"), 
        col("`_source_accounting date - quarter`").alias("_source_accounting date - quarter"), 
        col("`_source_accounting date - year`").alias("_source_accounting date - year"), 
        col("`_source_cost center`").alias("_source_cost center"), 
        col("`_source_cost center number`").alias("_source_cost center number"), 
        col("`_tableau display mega category`").alias("_tableau display mega category")
    )
