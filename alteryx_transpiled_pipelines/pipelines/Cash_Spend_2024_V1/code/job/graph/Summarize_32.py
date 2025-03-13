from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Summarize_32(spark: SparkSession, in0: DataFrame) -> DataFrame:
    df1 = in0.groupBy(
        col("`company code number`").alias("company code number"), 
        col("`source_cost center`").alias("source_cost center"), 
        col("`source_accounting date - quarter`").alias("quarter"), 
        col("`source_mg code description`").alias("source_mg code description"), 
        col("GL"), 
        col("`GL Account Description Updated`").alias("GL Account Description Updated"), 
        col("`tableau display category`").alias("tableau display category"), 
        col("`hyperion sub mc`").alias("hyperion sub mc"), 
        col("`tableau display mega category`").alias("category"), 
        col(
            "PA#"
          )\
          .alias("Planning Account"), 
        col("`hyperion evp`").alias("evp"), 
        col("`Planning Account`").alias("Planning Account Description"), 
        col("`source_cost center number`").alias("source_cost center number"), 
        col("`Regrouped Level 4`").alias("Regrouped Level 4"), 
        col("Site"), 
        col("`source_mg code`").alias("source_mg code"), 
        col("`source_accounting date - year`").alias("year"), 
        col("`CTS v3`").alias("CTS v3"), 
        col("`hyperion hlmc`").alias("hyperion hlmc"), 
        col("`hyperion mc`").alias("hyperion mc"), 
        col("`Company Description`").alias("Company Description")
    )

    return df1.agg(
        sum(col("`Sum_Spend ($)`")).alias("Sum_Adjusted_Sum_LCLCFX"), 
        sum(col("`Non-Opex Adjustments`")).alias("Non-Opex Adjustments")
    )
