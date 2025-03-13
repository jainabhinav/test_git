from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Union_101_variable2(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("`In-Scope Final`").alias("_In-Scope Final"), 
        col("Sum_Adjusted_Sum_LCLCFX").alias("_Sum_Adjusted_Sum_LCLCFX"), 
        col("`Regrouped Level 4`").alias("_Regrouped Level 4"), 
        col("company").alias("_company"), 
        col("hlmc").alias("_hlmc"), 
        col("year").alias("_year"), 
        col("`pa account`").alias("_pa account"), 
        col("`cost center`").alias("_cost center"), 
        col("`cost center number`").alias("_cost center number"), 
        col("`company code`").alias("_company code"), 
        col("Site").alias("_Site"), 
        col("mc").alias("_mc"), 
        col("`material group`").alias("_material group"), 
        col("`gl account`").alias("_gl account"), 
        col("`sub mc`").alias("_sub mc"), 
        col("`scenario name`").alias("_scenario name"), 
        col("`gl account number`").alias("_gl account number"), 
        col("`material group description`").alias("_material group description"), 
        col("quarter").alias("_quarter"), 
        col("`pa account desc`").alias("_pa account desc"), 
        col("evp").alias("_evp"), 
        col("category").alias("_category")
    )
