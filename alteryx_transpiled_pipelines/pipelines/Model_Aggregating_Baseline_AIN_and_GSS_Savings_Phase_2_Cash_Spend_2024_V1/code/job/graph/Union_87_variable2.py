from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Union_87_variable2(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("`In-Scope Final`").alias("_In-Scope Final"), 
        col("Sum_Adjusted_Sum_LCLCFX").alias("_Sum_Adjusted_Sum_LCLCFX"), 
        col("`Company Group`").alias("_Company Group"), 
        col("version").alias("_version"), 
        col("`Planning Account Description`").alias("_Planning Account Description"), 
        col("`planning sku`").alias("_planning sku"), 
        col("`regrouped level 4`").alias("_Regrouped Level 4"), 
        col("hlmc").alias("_hlmc"), 
        col("year").alias("_year"), 
        col("`scenario type`").alias("_scenario type"), 
        col("`cost center`").alias("_cost center"), 
        col("`cost center number`").alias("_cost center number"), 
        col("`Planning Account`").alias("_Planning Account"), 
        col("`Company Code`").alias("_Company Code"), 
        col("Sum_Adjusted_Sum_USDAFX").alias("_Sum_Adjusted_Sum_USDAFX"), 
        col("site").alias("_site"), 
        col("mc").alias("_mc"), 
        col("`Hyperion Category Code`").alias("_Hyperion Category Code"), 
        col("`material group`").alias("_material group"), 
        col("`gl account`").alias("_gl account"), 
        col("`sub mc`").alias("_sub mc"), 
        col("`product description`").alias("_product description"), 
        col("`scenario name`").alias("_scenario name"), 
        col("`gl account number`").alias("_gl account number"), 
        col("`Company Description`").alias("_Company Description"), 
        col("`material group description`").alias("_material group description"), 
        col("quarter").alias("_quarter"), 
        col("Sum_Adjusted_Sum_LCL").alias("_Sum_Adjusted_Sum_LCL"), 
        col("`Hyperion Category`").alias("_Hyperion Category"), 
        col("evp").alias("_evp"), 
        col("`Cash Spend`").alias("_Cash Spend"), 
        col("Category").alias("_Category")
    )
