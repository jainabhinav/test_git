from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Formula_28(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("`company code number`").alias("company code number"), 
        col("`source_cost center`").alias("source_cost center"), 
        col("`source_accounting date - quarter`").alias("source_accounting date - quarter"), 
        col("`GL Number`").alias("GL Number"), 
        col("Category"), 
        col("`Gl Account`").alias("Gl Account"), 
        col("`Category Old`").alias("Category Old"), 
        col("GL"), 
        col(
            "`Gl Account (# only)`"
          )\
          .alias(
          "Gl Account (# only)"
        ), 
        col("`tableau display mega category`").alias("tableau display mega category"), 
        col("`CTS v3 CC`").alias("CTS v3 CC"), 
        col(
          "PA#"
        ), 
        col("`hyperion evp`").alias("hyperion evp"), 
        col("`CC Lookup`").alias("CC Lookup"), 
        col("`Planning Account`").alias("Planning Account"), 
        col("`Sum_Sum_Spend ($)`").alias("Sum_Sum_Spend ($)"), 
        col("`source_cost center number`").alias("source_cost center number"), 
        col("`Regrouped Level 4`").alias("Regrouped Level 4"), 
        col("`source_accounting date - year`").alias("source_accounting date - year"), 
        col("`CTS v3`").alias("CTS v3"), 
        col("`Mega Category Old`").alias("Mega Category Old"), 
        col("`hyperion hlmc`").alias("hyperion hlmc"), 
        col("`hyperion mc`").alias("hyperion mc"), 
        lit("Other").cast(StringType()).alias("OSE Labor")
    )
