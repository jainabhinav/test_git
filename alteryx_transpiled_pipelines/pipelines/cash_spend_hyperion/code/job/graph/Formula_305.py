from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Formula_305(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("`pa account desc`").alias("pa account desc"), 
        col("Category"), 
        col("`scenario name`").alias("scenario name"), 
        col("`company code`").alias("company code"), 
        col("`sub mc`").alias("sub mc"), 
        col("site"), 
        col("Sum_Adjusted_Sum_LCL"), 
        col("year"), 
        col("`material group`").alias("material group"), 
        col("quarter"), 
        col("Sum_Adjusted_Sum_LCLCFX"), 
        col("`hyperion category code`").alias("hyperion category code"), 
        col("company"), 
        col("version"), 
        col("`scenario type`").alias("scenario type"), 
        col("`planning sku`").alias("planning sku"), 
        col("`cost center number`").alias("cost center number"), 
        col("`gl account`").alias("gl account"), 
        col("`hyperion category`").alias("hyperion category"), 
        col("evp"), 
        col("`pa account`").alias("pa account"), 
        col("mc"), 
        col("hlmc"), 
        col("`regrouped level 4`").alias("regrouped level 4"), 
        col("`cost center`").alias("cost center"), 
        col("`product description`").alias("product description"), 
        col("Sum_Adjusted_Sum_USDAFX"), 
        col("`gl account number`").alias("gl account number"), 
        (col("`scenario name`") + col("Category")).cast(StringType()).alias("Split Divide"), 
        (
          (
            (
              (
                (
                  (
                    (
                      (
                        (
                          (
                            (
                              (
                                (
                                  (
                                    (
                                      (
                                        (
                                          (
                                            (
                                              (
                                                (
                                                  col("evp")
                                                  + col(
                                                    "`cost center number`"
                                                  )
                                                )
                                                + col("hlmc")
                                              )
                                              + col("mc")
                                            )
                                            + col("`cost center`")
                                          )
                                          + col("quarter")
                                        )
                                        + col("site")
                                      )
                                      + col("`regrouped level 4`")
                                    )
                                    + col("`hyperion category`")
                                  )
                                  + col("`hyperion category code`")
                                )
                                + col("`pa account`")
                              )
                              + col("`pa account desc`")
                            )
                            + col("company")
                          )
                          + col("`company code`")
                        )
                        + col("`scenario type`")
                      )
                      + col("`gl account`")
                    )
                    + col("`gl account number`")
                  )
                  + col("`sub mc`")
                )
                + col("`material group`")
              )
              + col("`planning sku`")
            )
            + col("`product description`")
          )
          + col("version")
        )\
          .cast(StringType())\
          .alias("Group")
    )
