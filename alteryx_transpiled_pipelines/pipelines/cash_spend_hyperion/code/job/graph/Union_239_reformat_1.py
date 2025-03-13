from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Union_239_reformat_1(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("`CTS v3`").alias("CTS v3"), 
        col("Category"), 
        col("`Category Old`").alias("Category Old"), 
        col("Flag"), 
        col("GL"), 
        col("`GL Account Description Updated`").alias("GL Account Description Updated"), 
        col("`Gl Account`").alias("Gl Account"), 
        col(
            "`Gl Account (# only)`"
          )\
          .alias(
          "Gl Account (# only)"
        ), 
        col("`Mega Category Old`").alias("Mega Category Old"), 
        col("`Planning Account`").alias("Planning Account"), 
        col("`Regrouped Level 4`").alias("Regrouped Level 4"), 
        col("Site"), 
        col("`Sum_Spend ($)`").alias("Sum_Spend ($)"), 
        col("`company code name`").alias("company code name"), 
        col("`company code number`").alias("company code number"), 
        col("`hyperion evp`").alias("hyperion evp"), 
        col("`hyperion hlmc`").alias("hyperion hlmc"), 
        col("`hyperion mc`").alias("hyperion mc"), 
        col("`hyperion sub mc`").alias("hyperion sub mc"), 
        col("`source_accounting date - quarter`").alias("source_accounting date - quarter"), 
        col("`source_accounting date - year`").alias("source_accounting date - year"), 
        col("`source_cost center`").alias("source_cost center"), 
        col("`source_cost center number`").alias("source_cost center number"), 
        col("`source_mg code`").alias("source_mg code"), 
        col("`source_mg code description`").alias("source_mg code description"), 
        col("`tableau display category`").alias("tableau display category"), 
        col("`tableau display mega category`").alias("tableau display mega category")
    )
