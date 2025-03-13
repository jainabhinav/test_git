from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Union_409_variable1(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("version").alias("_version"), 
        col("`planning sku`").alias("_planning sku"), 
        col("`regrouped level 4`").alias("_regrouped level 4"), 
        col("company").alias("_company"), 
        col("hlmc").alias("_hlmc"), 
        col("year").alias("_year"), 
        col("`pa account`").alias("_pa account"), 
        col("Sum_USDAFX").alias("_Sum_USDAFX"), 
        col("`scenario type`").alias("_scenario type"), 
        col("`cost center`").alias("_cost center"), 
        col("`cost center number`").alias("_cost center number"), 
        col("`Exclusion Type`").alias("_Exclusion Type"), 
        col("`company code`").alias("_company code"), 
        col("Sum_LCL").alias("_Sum_LCL"), 
        col("site").alias("_site"), 
        col("Sum_LCLCFX").alias("_Sum_LCLCFX"), 
        col("mc").alias("_mc"), 
        col("`category code`").alias("_category code"), 
        col("`material group`").alias("_material group"), 
        col("`gl account`").alias("_gl account"), 
        col("`sub mc`").alias("_sub mc"), 
        col("`product description`").alias("_product description"), 
        col("`scenario name`").alias("_scenario name"), 
        col("`gl account number`").alias("_gl account number"), 
        col("quarter").alias("_quarter"), 
        col("`pa account desc`").alias("_pa account desc"), 
        col("evp").alias("_evp"), 
        col("category").alias("_category")
    )
