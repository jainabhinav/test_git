from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Join_382_inner(spark: SparkSession, in0: DataFrame, in1: DataFrame, ) -> DataFrame:
    return in0\
        .alias("in0")\
        .join(
          in1.alias("in1"),
          ((col("in0.`gl account number`") == col("in1.GL")) & (col("in0.`CTS v3`") == col("in1.Name"))),
          "inner"
        )\
        .select(col("in0.`pa account desc`").alias("pa account desc"), col("in1.Name").alias("Name"), col("in1.`OSE Labor`").alias("OSE Labor"), col("in1.Description").alias("Description"), col("in0.`scenario name`").alias("scenario name"), col("in0.`company code`").alias("company code"), col("in0.`sub mc`").alias("sub mc"), col("in0.Site").alias("Site"), col("in0.`Non-Opex Adjustments`").alias("Non-Opex Adjustments"), col("in0.year").alias("year"), col("in0.`material group`").alias("material group"), col("in0.quarter").alias("quarter"), col("in0.company").alias("company"), col("in0.`cost center number`").alias("cost center number"), col("in0.`gl account`").alias("gl account"), col("in0.evp").alias("evp"), col("in0.`CTS v3`").alias("CTS v3"), col("in0.`pa account`").alias("pa account"), col("in0.category").alias("category"), col("in0.`material group description`").alias("material group description"), col("in0.mc").alias("mc"), col("in0.hlmc").alias("hlmc"), col("in0.`Regrouped Level 4`").alias("Regrouped Level 4"), col("in1.GL").alias("Right_GL"), col("in0.`cost center`").alias("cost center"), col("in0.Sum_Adjusted_Sum_LCLCFX").alias("Sum_Adjusted_Sum_LCLCFX"), col("in0.`gl account number`").alias("gl account number"))
