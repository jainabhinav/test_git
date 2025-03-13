from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Union_261_reformat_0(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("`_CTS v3`").alias("_CTS v3"), 
        col("_Category").cast(StringType()).alias("_Category"), 
        col("`_Category Old`").alias("_Category Old"), 
        lit(None).cast(StringType()).alias("_Flag"), 
        col("_GL"), 
        col("`_GL Account Description Updated`").alias("_GL Account Description Updated"), 
        col("`_Gl Account`").cast(StringType()).alias("_Gl Account"), 
        col(
            "`_Gl Account (# only)`"
          )\
          .cast(StringType())\
          .alias(
          "_Gl Account (# only)"
        ), 
        col("`_Mega Category Old`").alias("_Mega Category Old"), 
        lit(None).cast(DoubleType()).alias("_Non-Opex Adjustments"), 
        col(
            "_PA#"
          )\
          .cast(StringType())\
          .alias(
          "_PA#"
        ), 
        col("`_Planning Account`").cast(StringType()).alias("_Planning Account"), 
        col("`_Regrouped Level 4`").cast(StringType()).alias("_Regrouped Level 4"), 
        col("_Site"), 
        col("`_Sum_Spend ($)`").cast(DoubleType()).alias("_Sum_Spend ($)"), 
        col("`_company code name`").alias("_company code name"), 
        col("`_company code number`").alias("_company code number"), 
        col("`_hyperion evp`").alias("_hyperion evp"), 
        col("`_hyperion hlmc`").alias("_hyperion hlmc"), 
        col("`_hyperion mc`").alias("_hyperion mc"), 
        col("`_hyperion sub mc`").alias("_hyperion sub mc"), 
        col("`_source_accounting date - quarter`").alias("_source_accounting date - quarter"), 
        col("`_source_accounting date - year`").alias("_source_accounting date - year"), 
        col("`_source_cost center`").alias("_source_cost center"), 
        col("`_source_cost center number`").alias("_source_cost center number"), 
        col("`_source_mg code`").alias("_source_mg code"), 
        col("`_source_mg code description`").alias("_source_mg code description"), 
        col("`_tableau display category`").alias("_tableau display category"), 
        col("`_tableau display mega category`").alias("_tableau display mega category")
    )
