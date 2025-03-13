from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def AlteryxSelect_34(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("Name"), 
        col("`Mega Category Old`").alias("Mega Category Old"), 
        col("`company code number`").alias("company code number"), 
        col("`source_cost center`").alias("source_cost center"), 
        col("`source_accounting date - quarter`").alias("source_accounting date - quarter"), 
        col("`OSE Labor`").alias("OSE Labor"), 
        col("`Right_CC Lookup`").alias("Right_CC Lookup"), 
        col("Description"), 
        col("GL"), 
        col("`CC Lookup`").alias("CC Lookup"), 
        col("ASHB"), 
        col("`tableau display mega category`").alias("tableau display mega category"), 
        col("`Category Old`").alias("Category Old"), 
        col("`hyperion evp`").alias("hyperion evp"), 
        col("`Gl Account`").alias("Gl Account"), 
        col("`source_cost center number`").alias("source_cost center number"), 
        col("`CTS v3`").alias("CTS v3"), 
        col("`Sum_Sum_Spend ($)`").alias("Sum_Sum_Spend ($)"), 
        col("Category"), 
        col("`Regrouped Level 4`").alias("Regrouped Level 4"), 
        col("`GL Number`").alias("GL Number"), 
        col(
            "`Gl Account (# only)`"
          )\
          .alias(
          "Gl Account (# only)"
        ), 
        col("`source_accounting date - year`").alias("source_accounting date - year"), 
        col("Right_GL"), 
        col("`Planning Account`").alias("Planning Account"), 
        col("`hyperion hlmc`").alias("hyperion hlmc"), 
        col(
          "PA#"
        ), 
        col("`hyperion mc`").alias("hyperion mc")
    )
