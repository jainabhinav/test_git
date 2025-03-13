from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Filter_325(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.filter(
        (
          (col("`regrouped level 4`") == lit("Staff Support")).cast(BooleanType())
          | (col("`regrouped level 4`") == lit("Outside Expenses")).cast(BooleanType())
        )
    )
