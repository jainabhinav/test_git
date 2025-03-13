from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Formula_237(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("`company code number`").alias("company code number"), 
        col("`source_cost center`").alias("source_cost center"), 
        col("`source_accounting date - quarter`").alias("source_accounting date - quarter"), 
        col("Category"), 
        col("`source_mg code description`").alias("source_mg code description"), 
        col("`Gl Account`").alias("Gl Account"), 
        col("`Category Old`").alias("Category Old"), 
        col("`Sum_Spend ($)`").alias("Sum_Spend ($)"), 
        col("GL"), 
        col("`GL Account Description Updated`").alias("GL Account Description Updated"), 
        col("`tableau display category`").alias("tableau display category"), 
        col(
            "`Gl Account (# only)`"
          )\
          .alias(
          "Gl Account (# only)"
        ), 
        col("`hyperion sub mc`").alias("hyperion sub mc"), 
        col("`tableau display mega category`").alias("tableau display mega category"), 
        col("`hyperion evp`").alias("hyperion evp"), 
        col("`Planning Account`").alias("Planning Account"), 
        col("`source_cost center number`").alias("source_cost center number"), 
        col("`Regrouped Level 4`").alias("Regrouped Level 4"), 
        col("Site"), 
        col("`source_mg code`").alias("source_mg code"), 
        col("`source_accounting date - year`").alias("source_accounting date - year"), 
        col("`CTS v3`").alias("CTS v3"), 
        col("`Mega Category Old`").alias("Mega Category Old"), 
        col("`hyperion hlmc`").alias("hyperion hlmc"), 
        col("`company code name`").alias("company code name"), 
        col("`hyperion mc`").alias("hyperion mc"), 
        lit("Included").cast(StringType()).alias("Flag")
    )
