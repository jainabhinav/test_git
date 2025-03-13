from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Union_93_cleanup(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("`_pa account desc`").alias("pa account desc"), 
        col("_Name").alias("Name"), 
        col("`_OSE Labor`").alias("OSE Labor"), 
        col("`_Right_CC Lookup`").alias("Right_CC Lookup"), 
        col("_Description").alias("Description"), 
        col("`_scenario name`").alias("scenario name"), 
        col("`_company code`").alias("company code"), 
        col("`_sub mc`").alias("sub mc"), 
        col("_Site").alias("Site"), 
        col("`_Non-Opex Adjustments`").alias("Non-Opex Adjustments"), 
        col("_year").alias("year"), 
        col("`_material group`").alias("material group"), 
        col("_ASHB").alias("ASHB"), 
        col("`_tableau display category`").alias("tableau display category"), 
        col("_quarter").alias("quarter"), 
        col("_company").alias("company"), 
        col("`_cost center number`").alias("cost center number"), 
        col("`_gl account`").alias("gl account"), 
        col("_evp").alias("evp"), 
        col("`_CTS v3`").alias("CTS v3"), 
        col("`_pa account`").alias("pa account"), 
        col("_category").alias("category"), 
        col("`_material group description`").alias("material group description"), 
        col("_mc").alias("mc"), 
        col("_hlmc").alias("hlmc"), 
        col("`_Regrouped Level 4`").alias("Regrouped Level 4"), 
        col("_Right_GL").alias("Right_GL"), 
        col("`_cost center`").alias("cost center"), 
        col("_Sum_Adjusted_Sum_LCLCFX").alias("Sum_Adjusted_Sum_LCLCFX"), 
        col("`_gl account number`").alias("gl account number")
    )
