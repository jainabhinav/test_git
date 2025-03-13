from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Formula_421(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("`pa account desc`").alias("pa account desc"), 
        col("ASHB"), 
        col("Category"), 
        col("`scenario name`").alias("scenario name"), 
        col("`company code`").alias("company code"), 
        col("`sub mc`").alias("sub mc"), 
        col("site"), 
        col("year"), 
        col("`material group`").alias("material group"), 
        col("`OSE Labor`").alias("OSE Labor"), 
        col("quarter"), 
        col("Sum_Sum_Adjusted_Sum_LCLCFX"), 
        col("`hyperion category code`").alias("hyperion category code"), 
        col("company"), 
        col("version"), 
        col("`scenario type`").alias("scenario type"), 
        col("`planning sku`").alias("planning sku"), 
        col("`cost center number`").alias("cost center number"), 
        col("`gl account`").alias("gl account"), 
        col("`hyperion category`").alias("hyperion category"), 
        col("evp"), 
        col("`Split Divide`").alias("Split Divide"), 
        col("`pa account`").alias("pa account"), 
        col("`material group description`").alias("material group description"), 
        col("mc"), 
        col("hlmc"), 
        col("`regrouped level 4`").alias("regrouped level 4"), 
        col("`CTS v3`").alias("CTS v3"), 
        col("`cost center`").alias("cost center"), 
        col("`product description`").alias("product description"), 
        col("`gl account number`").alias("gl account number"), 
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
                        + col("`company code`")
                      )
                      + col("company")
                    )
                    + col("`scenario type`")
                  )
                  + col("`gl account`")
                )
                + col("`gl account number`")
              )
              + col("year")
            )
            + col("`scenario name`")
          )
          + col("Category")
        )\
          .cast(StringType())\
          .alias("Group")
    )
