from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Transpose_40_schemaTransform(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("`Savings Type`").alias("Savings Type"), 
        col("`Operations-2027`").alias("Operations-2027"), 
        col("`R&D/MA-2025`").alias("R&D/MA-2025"), 
        col("`G&A-2025`").alias("G&A-2025"), 
        col("`R&D/MA-2026`").alias("R&D/MA-2026"), 
        col("`Mega Category`").alias("Mega Category"), 
        col("`Other-2025`").alias("Other-2025"), 
        col("`G&A-2026`").alias("G&A-2026"), 
        col("`S&M/Int'l-2027`").alias("S&M/Int'l-2027"), 
        col("`Other-2026`").alias("Other-2026"), 
        col("`Operations-2025`").alias("Operations-2025"), 
        col("`S&M/Int'l-2026`").alias("S&M/Int'l-2026"), 
        col("`R&D/MA-2027`").alias("R&D/MA-2027"), 
        col("`G&A-2027`").alias("G&A-2027"), 
        col("`Operations-2026`").alias("Operations-2026"), 
        col("`S&M/Int'l-2025`").alias("S&M/Int'l-2025"), 
        col("`Other-2027`").alias("Other-2027")
    )
