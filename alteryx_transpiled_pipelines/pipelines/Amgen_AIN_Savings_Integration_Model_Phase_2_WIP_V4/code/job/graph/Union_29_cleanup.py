from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Union_29_cleanup(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("_Name").alias("Name"), 
        col("`_Mega Category Old`").alias("Mega Category Old"), 
        col("`_company code number`").alias("company code number"), 
        col("`_source_cost center`").alias("source_cost center"), 
        col("`_source_accounting date - quarter`").alias("source_accounting date - quarter"), 
        col("`_OSE Labor`").alias("OSE Labor"), 
        col("_Description").alias("Description"), 
        col("_GL").alias("GL"), 
        col("`_CC Lookup`").alias("CC Lookup"), 
        col("`_tableau display mega category`").alias("tableau display mega category"), 
        col("`_Category Old`").alias("Category Old"), 
        col("`_hyperion evp`").alias("hyperion evp"), 
        col("`_Gl Account`").alias("Gl Account"), 
        col("`_source_cost center number`").alias("source_cost center number"), 
        col("`_CTS v3`").alias("CTS v3"), 
        col("`_Sum_Sum_Spend ($)`").alias("Sum_Sum_Spend ($)"), 
        col("_Category").alias("Category"), 
        col("`_Regrouped Level 4`").alias("Regrouped Level 4"), 
        col("`_GL Number`").alias("GL Number"), 
        col(
            "`_Gl Account (# only)`"
          )\
          .alias(
          "Gl Account (# only)"
        ), 
        col("`_source_accounting date - year`").alias("source_accounting date - year"), 
        col("_Right_GL").alias("Right_GL"), 
        col("`_CTS v3 CC`").alias("CTS v3 CC"), 
        col("`_Planning Account`").alias("Planning Account"), 
        col("`_hyperion hlmc`").alias("hyperion hlmc"), 
        col(
            "_PA#"
          )\
          .alias(
          "PA#"
        ), 
        col("`_hyperion mc`").alias("hyperion mc")
    )
