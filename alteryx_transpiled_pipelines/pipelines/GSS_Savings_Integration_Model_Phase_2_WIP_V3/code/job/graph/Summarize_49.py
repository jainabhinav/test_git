from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Summarize_49(spark: SparkSession, in0: DataFrame) -> DataFrame:
    df1 = in0.groupBy(col("`Mega Category`").alias("Mega Category"), col("Year"))

    return df1.agg(
        sum(col("`% Allocation - BAU`")).alias("Sum_% Allocation - BAU"), 
        sum(col("`% Allocation - Incremental`")).alias("Sum_% Allocation - Incremental")
    )
