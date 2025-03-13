from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Formula_411(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("`Country Short`").alias("Country Short"), 
        col("`fit supplier diversity flag`").alias("fit supplier diversity flag"), 
        col("`po line number`").alias("po line number"), 
        col("`source_procurement channel code - procurement channel code`")\
          .alias("source_procurement channel code - procurement channel code"), 
        col("`company code number`").alias("company code number"), 
        col("`source_cost center`").alias("source_cost center"), 
        col("_version"), 
        col("`hyperion sub mc3`").alias("hyperion sub mc3"), 
        col("`hyperion cc_function_descr`").alias("hyperion cc_function_descr"), 
        col("`hyperion function description`").alias("hyperion function description"), 
        col("`source_accounting date - quarter`").alias("source_accounting date - quarter"), 
        col("`_planning sku`").alias("_planning sku"), 
        col("`source_amgen region/country - world sub-region_temp`")\
          .alias("source_amgen region/country - world sub-region_temp"), 
        col("`source_mg code description`").alias("source_mg code description"), 
        col("ParentVendor_match_flag"), 
        col("`source_wbs - accounting treatment`").alias("source_wbs - accounting treatment"), 
        col("`source_amgen region/country - world sub-region`").alias("source_amgen region/country - world sub-region"), 
        col("Country"), 
        col("Country_match_flag"), 
        col("`Country Old`").alias("Country Old"), 
        col("`source_sap_vendor description`").alias("source_sap_vendor description"), 
        col("`Category Old`").alias("Category Old"), 
        col("_Sum_LCL"), 
        col("`tableau display material group`").alias("tableau display material group"), 
        col("_Sum_USDAFX"), 
        col("Sub_group_flag"), 
        col("`SUB GROUP`").alias("SUB GROUP"), 
        col("`po id`").alias("po id"), 
        col("`_scenario type`").alias("_scenario type"), 
        col("`source_vendor - vendor global ultimate parent (enriched)`")\
          .alias("source_vendor - vendor global ultimate parent (enriched)"), 
        col("`source_vendor - cc3 - veteran owned small bus`").alias("source_vendor - cc3 - veteran owned small bus"), 
        col("`source_accounting date - month`").alias("source_accounting date - month"), 
        col("`source_wbs - description`").alias("source_wbs - description"), 
        col("`source_vendor - cc1 - type`").alias("source_vendor - cc1 - type"), 
        col("`source_invoice spend (actual fx rate)`").alias("source_invoice spend (actual fx rate)"), 
        col("`hyperion function level 4`").alias("hyperion function level 4"), 
        col("`_Gl Account`").alias("_Gl Account"), 
        col("`vendor country`").alias("vendor country"), 
        col("source_vendor_number"), 
        col("`source_invoice number`").alias("source_invoice number"), 
        col("`GL Account Description Updated`").alias("GL Account Description Updated"), 
        col("`source_vendor - cc3 - svc disabled vet owned sb`")\
          .alias("source_vendor - cc3 - svc disabled vet owned sb"), 
        col("`tableau display category`").alias("tableau display category"), 
        col("`source_vendor - cc4 - obs-foreign supplier`").alias("source_vendor - cc4 - obs-foreign supplier"), 
        col("`source_vendor - vendor global ultimate parent (enriched) temp1`")\
          .alias("source_vendor - vendor global ultimate parent (enriched) temp1"), 
        col("`hyperion sub mc2`").alias("hyperion sub mc2"), 
        col("`hyperion sub mc`").alias("hyperion sub mc"), 
        col("`source_vendor - cc2 - women owned small bus`").alias("source_vendor - cc2 - women owned small bus"), 
        col("`tableau display mega category`").alias("tableau display mega category"), 
        col("`source_vendor - vendor global ultimate parent (enriched) old`")\
          .alias("source_vendor - vendor global ultimate parent (enriched) old"), 
        col("`source_vendor - cc5 - small business`").alias("source_vendor - cc5 - small business"), 
        col("`source_amgen region/country - world sub-region Old`")\
          .alias("source_amgen region/country - world sub-region Old"), 
        col("`vendor country code`").alias("vendor country code"), 
        col(
            "`_Gl Account (# only)`"
          )\
          .alias(
          "_Gl Account (# only)"
        ), 
        col("`source_vendor - vendor (enriched)`").alias("source_vendor - vendor (enriched)"), 
        col("_Category"), 
        col("`Sum of Commitment Amount`").alias("Sum of Commitment Amount"), 
        col("`hyperion function level 3`").alias("hyperion function level 3"), 
        col("`hyperion evp`").alias("hyperion evp"), 
        col("`_Planning Account`").alias("_Planning Account"), 
        col("_GL"), 
        col("`source_invoice spend (budget fx rate)`").alias("source_invoice spend (budget fx rate)"), 
        col("`source_requester - user/buyer name responsible for spend`")\
          .alias("source_requester - user/buyer name responsible for spend"), 
        col("`source_vendor - cc4 - hubzone small business`").alias("source_vendor - cc4 - hubzone small business"), 
        col("`sap/concur source`").alias("sap/concur source"), 
        col("`source_amgen region/country - world region`").alias("source_amgen region/country - world region"), 
        col("`_category code`").alias("_category code"), 
        col("`source_invoice spend (original currency)`").alias("source_invoice spend (original currency)"), 
        col("`_product description`").alias("_product description"), 
        col("`source_cost center number`").alias("source_cost center number"), 
        col("`source_vendor - cc1 - indicator`").alias("source_vendor - cc1 - indicator"), 
        col("`hyperion cc description`").alias("hyperion cc description"), 
        col("`invoice linenumber`").alias("invoice linenumber"), 
        col("`source_amgen region/country - sap company code`").alias("source_amgen region/country - sap company code"), 
        col("`hyperion function level 2`").alias("hyperion function level 2"), 
        col("`_Regrouped Level 4`").alias("_Regrouped Level 4"), 
        col("`hyperion cc_function_mapping`").alias("hyperion cc_function_mapping"), 
        col("`source_vendor - vendor global ultimate parent (enriched) temp`")\
          .alias("source_vendor - vendor global ultimate parent (enriched) temp"), 
        col("`SUB-GROUP_Temp`").alias("SUB-GROUP_Temp"), 
        col("`source_gl number`").alias("source_gl number"), 
        col("`source_wbs - product name`").alias("source_wbs - product name"), 
        col("Site"), 
        col("`po description`").alias("po description"), 
        col("`source_mg code`").alias("source_mg code"), 
        col("`tableau display meta category`").alias("tableau display meta category"), 
        col("`source_accounting date - year`").alias("source_accounting date - year"), 
        col("`source_amgen region/country - country`").alias("source_amgen region/country - country"), 
        col("`Mega Category Old`").alias("Mega Category Old"), 
        col("`hyperion hlmc`").alias("hyperion hlmc"), 
        col("`company code name`").alias("company code name"), 
        col("`source_invoice quantity`").alias("source_invoice quantity"), 
        col("`hyperion mc`").alias("hyperion mc"), 
        col("`source_vendor - cc5 - obs-large business`").alias("source_vendor - cc5 - obs-large business"), 
        col("`source_accounting date`").alias("source_accounting date"), 
        col("`Company Description`").alias("Company Description"), 
        lit("Non-Addressable Mega Category").cast(StringType()).alias("Exclusion Type"), 
        when(
            (locate("", col("`GL Account Description Updated`")) > lit(0)).cast(BooleanType()), 
            call_spark_fcn(
              "string_substring", 
              col("`GL Account Description Updated`"), 
              lit(1), 
              locate("", col("`GL Account Description Updated`"))
            )
          )\
          .otherwise(col("`GL Account Description Updated`"))\
          .cast(StringType())\
          .alias("GL")
    )
