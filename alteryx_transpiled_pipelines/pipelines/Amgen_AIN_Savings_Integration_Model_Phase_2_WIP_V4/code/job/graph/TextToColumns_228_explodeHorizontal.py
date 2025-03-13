from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def TextToColumns_228_explodeHorizontal(spark: SparkSession, in0: DataFrame) -> DataFrame:
    df1 = in0.groupBy(col("Function"), col("Name"), col("Value"), col("_seq"))
    df2 = df1.pivot("_pivot", ["Name1", "Name2", "Name3"])

    return df2.agg(max(col("_temp_Name")).alias("_temp_Name"))
