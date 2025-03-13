from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Join_36_inner(spark: SparkSession, in0: DataFrame, in1: DataFrame, ) -> DataFrame:
    return in0\
        .alias("in0")\
        .join(in1.alias("in1"), (col("in0.`CTS v3`") == col("in1.`CTS V3`")), "inner")\
        .select(
        col("in1.`In-Scope?`").alias("In-Scope?"), 
        col("in0.Name").alias("Name"), 
        col("in0.`Mega Category Old`").alias("Mega Category Old"), 
        col("in0.`company code number`").alias("company code number"), 
        col("in0.`source_cost center`").alias("source_cost center"), 
        col("in0.`source_accounting date - quarter`").alias("source_accounting date - quarter"), 
        col("in0.`OSE Labor`").alias("OSE Labor"), 
        col("in0.`Right_CC Lookup`").alias("Right_CC Lookup"), 
        col("in0.Description").alias("Description"), 
        col("in0.GL").alias("GL"), 
        col("in0.`CC Lookup`").alias("CC Lookup"), 
        col("in0.ASHB").alias("ASHB"), 
        col("in0.`tableau display mega category`").alias("tableau display mega category"), 
        col("in0.`Category Old`").alias("Category Old"), 
        col("in0.`hyperion evp`").alias("hyperion evp"), 
        col("in1.`CTS V3`").alias("Right_CTS V3"), 
        col("in0.`Gl Account`").alias("Gl Account"), 
        col("in1.`Function as per AIN Business Case Team`").alias("Function as per AIN Business Case Team"), 
        col("in0.`source_cost center number`").alias("source_cost center number"), 
        col("in0.`CTS v3`").alias("CTS v3"), 
        col("in0.`Sum_Sum_Spend ($)`").alias("Sum_Sum_Spend ($)"), 
        col("in0.Category").alias("Category"), 
        col("in0.`Regrouped Level 4`").alias("Regrouped Level 4"), 
        col("in0.`GL Number`").alias("GL Number"), 
        col(
            "in0.`Gl Account (# only)`"
          )\
          .alias(
          "Gl Account (# only)"
        ), 
        col("in0.`source_accounting date - year`").alias("source_accounting date - year"), 
        col("in0.Right_GL").alias("Right_GL"), 
        col("in0.`CTS v3 CC`").alias("CTS v3 CC"), 
        col("in0.`Planning Account`").alias("Planning Account"), 
        col("in0.`hyperion hlmc`").alias("hyperion hlmc"), 
        col(
            "in0.PA#"
          )\
          .alias(
          "PA#"
        ), 
        col("in0.`hyperion mc`").alias("hyperion mc")
    )
