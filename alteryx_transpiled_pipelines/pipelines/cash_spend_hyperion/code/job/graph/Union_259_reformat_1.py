from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Union_259_reformat_1(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("Flag"), 
        col("LCL"), 
        col("LCLCFX"), 
        col("`PA Account`").alias("PA Account"), 
        col("USDAFX"), 
        col("`_material group description`").alias("_material group description"), 
        col("`acct level 3`").alias("acct level 3"), 
        col("`acct level 4`").alias("acct level 4"), 
        col("`acct level 5`").alias("acct level 5"), 
        col("category"), 
        col("`category code`").alias("category code"), 
        col("company"), 
        col("`company code`").alias("company code"), 
        col("`cost center`").alias("cost center"), 
        col("`cost center number`").alias("cost center number"), 
        col("evp"), 
        col("`gl account`").alias("gl account"), 
        col("`gl account number`").alias("gl account number"), 
        col("hlmc"), 
        col("`local currency`").alias("local currency"), 
        col("`material group`").alias("material group"), 
        col("mc"), 
        col("`pa account desc`").alias("pa account desc"), 
        col("`planning sku`").alias("planning sku"), 
        col("`product description`").alias("product description"), 
        col("quarter"), 
        col("`regrouped level 4`").alias("regrouped level 4"), 
        col("`scenario name`").alias("scenario name"), 
        col("`scenario type`").alias("scenario type"), 
        col("site"), 
        col("`site detail`").alias("site detail"), 
        col("site_nonsite"), 
        col("`sub mc`").alias("sub mc"), 
        col("`sub mc2`").alias("sub mc2"), 
        col("version"), 
        col("year")
    )
