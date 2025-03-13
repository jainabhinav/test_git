from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Union_261_variable2(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("`CTS v3`").alias("_CTS v3"), 
        col("`source_mg code description`").alias("_source_mg code description"), 
        col("`company code name`").alias("_company code name"), 
        col("`Regrouped Level 4`").alias("_Regrouped Level 4"), 
        col("GL").alias("_GL"), 
        col("`Sum_Spend ($)`").alias("_Sum_Spend ($)"), 
        col("`tableau display mega category`").alias("_tableau display mega category"), 
        col("`source_accounting date - year`").alias("_source_accounting date - year"), 
        col("`source_mg code`").alias("_source_mg code"), 
        col("`tableau display category`").alias("_tableau display category"), 
        col("`hyperion sub mc`").alias("_hyperion sub mc"), 
        col(
            "PA#"
          )\
          .alias(
          "_PA#"
        ), 
        col("`Planning Account`").alias("_Planning Account"), 
        col("Site").alias("_Site"), 
        col("`source_cost center`").alias("_source_cost center"), 
        col("`source_accounting date - quarter`").alias("_source_accounting date - quarter"), 
        col("`GL Account Description Updated`").alias("_GL Account Description Updated"), 
        col("`Gl Account`").alias("_Gl Account"), 
        col("`Mega Category Old`").alias("_Mega Category Old"), 
        col("`company code number`").alias("_company code number"), 
        col("`hyperion mc`").alias("_hyperion mc"), 
        col(
            "`Gl Account (# only)`"
          )\
          .alias(
          "_Gl Account (# only)"
        ), 
        col("`Category Old`").alias("_Category Old"), 
        col("`hyperion evp`").alias("_hyperion evp"), 
        col("`source_cost center number`").alias("_source_cost center number"), 
        col("`hyperion hlmc`").alias("_hyperion hlmc"), 
        col("Category").alias("_Category")
    )
