from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Join_18_inner(spark: SparkSession, in0: DataFrame, in1: DataFrame, ) -> DataFrame:
    return in0\
        .alias("in0")\
        .join(
          in1.alias("in1"),
          ((col("in0.`Split Divide`") == col("in1.`Split Divide`")) & (col("in0.Group") == col("in1.Group"))),
          "inner"
        )\
        .select(col("in1.%spilt").alias("%spilt"), col("in0.`OSE Labor`").alias("OSE Labor"), col("in0.`Planning Account Description`").alias("Planning Account Description"), col("in0.`scenario name`").alias("scenario name"), col("in0.`Company Code`").alias("Company Code"), col("in0.`sub mc`").alias("sub mc"), col("in0.site").alias("site"), col("in0.`Non-Opex Adjustments`").alias("Non-Opex Adjustments"), col("in0.`Cash Spend`").alias("Cash Spend"), col("in0.year").alias("year"), col("in0.`material group`").alias("material group"), col("in0.`Company Description`").alias("Company Description"), col("in0.ASHB").alias("ASHB"), col("in0.quarter").alias("quarter"), col("in0.`Hyperion Category Code`").alias("Hyperion Category Code"), col("in0.version").alias("version"), col("in0.`scenario type`").alias("scenario type"), col("in0.`planning sku`").alias("planning sku"), col("in0.Sum_Adjusted_Sum_USDAFX").alias("Sum_Adjusted_Sum_USDAFX"), col("in0.`cost center number`").alias("cost center number"), col("in0.`Planning Account Actual`").alias("Planning Account Actual"), col("in0.`gl account`").alias("gl account"), col("in0.RecordID").alias("RecordID"), col("in0.`Hyperion Category`").alias("Hyperion Category"), col("in0.evp").alias("evp"), col("in0.`CTS v3`").alias("CTS v3"), col("in0.Sum_Adjusted_Sum_LCL").alias("Sum_Adjusted_Sum_LCL"), col("in0.Category").alias("Category"), col("in0.`material group description`").alias("material group description"), col("in0.mc").alias("mc"), col("in0.hlmc").alias("hlmc"), col("in0.`Planning Account Description Actual`").alias("Planning Account Description Actual"), col("in0.`regrouped level 4`").alias("regrouped level 4"), col("in0.`cost center`").alias("cost center"), col("in0.`product description`").alias("product description"), col("in0.`Planning Account`").alias("Planning Account"), col("in0.Sum_Adjusted_Sum_LCLCFX").alias("Sum_Adjusted_Sum_LCLCFX"), col("in0.`gl account number`").alias("gl account number"))
