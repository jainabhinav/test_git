from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Union_100_reformat_2(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("`Category Old`").alias("Category Old"), 
        col("`Country Old`").alias("Country Old"), 
        col("`Country Short`").alias("Country Short"), 
        col("Country_match_flag"), 
        col("`GL Account Description Updated`").alias("GL Account Description Updated"), 
        col("`Mega Category Old`").alias("Mega Category Old"), 
        col("ParentVendor_match_flag"), 
        col("`SUB GROUP`").alias("SUB GROUP"), 
        col("Site"), 
        col("`Sum of Commitment Amount`").alias("Sum of Commitment Amount"), 
        col("`company code name`").alias("company code name"), 
        col("`company code number`").alias("company code number"), 
        col("`fit supplier diversity flag`").alias("fit supplier diversity flag"), 
        col("`hyperion cc description`").alias("hyperion cc description"), 
        col("`hyperion cc_function_descr`").alias("hyperion cc_function_descr"), 
        col("`hyperion cc_function_mapping`").alias("hyperion cc_function_mapping"), 
        col("`hyperion evp`").alias("hyperion evp"), 
        col("`hyperion function description`").alias("hyperion function description"), 
        col("`hyperion function level 2`").alias("hyperion function level 2"), 
        col("`hyperion function level 3`").alias("hyperion function level 3"), 
        col("`hyperion function level 4`").alias("hyperion function level 4"), 
        col("`hyperion hlmc`").alias("hyperion hlmc"), 
        col("`hyperion mc`").alias("hyperion mc"), 
        col("`hyperion sub mc`").alias("hyperion sub mc"), 
        col("`hyperion sub mc2`").alias("hyperion sub mc2"), 
        col("`hyperion sub mc3`").alias("hyperion sub mc3"), 
        col("`invoice linenumber`").alias("invoice linenumber"), 
        col("`po description`").alias("po description"), 
        col("`po id`").alias("po id"), 
        col("`po line number`").alias("po line number"), 
        col("`sap/concur source`").alias("sap/concur source"), 
        col("`source_accounting date`").alias("source_accounting date"), 
        col("`source_accounting date - month`").alias("source_accounting date - month"), 
        col("`source_accounting date - quarter`").alias("source_accounting date - quarter"), 
        col("`source_accounting date - year`").alias("source_accounting date - year"), 
        col("`source_amgen region/country - country`").alias("source_amgen region/country - country"), 
        col("`source_amgen region/country - sap company code`").alias("source_amgen region/country - sap company code"), 
        col("`source_amgen region/country - world region`").alias("source_amgen region/country - world region"), 
        col("`source_amgen region/country - world sub-region`").alias("source_amgen region/country - world sub-region"), 
        col("`source_amgen region/country - world sub-region Old`")\
          .alias("source_amgen region/country - world sub-region Old"), 
        col("`source_amgen region/country - world sub-region_temp`")\
          .alias("source_amgen region/country - world sub-region_temp"), 
        col("`source_cost center`").alias("source_cost center"), 
        col("`source_cost center number`").alias("source_cost center number"), 
        col("`source_gl number`").alias("source_gl number"), 
        col("`source_invoice number`").alias("source_invoice number"), 
        col("`source_invoice quantity`").alias("source_invoice quantity"), 
        col("`source_invoice spend (actual fx rate)`").alias("source_invoice spend (actual fx rate)"), 
        col("`source_invoice spend (budget fx rate)`").alias("source_invoice spend (budget fx rate)"), 
        col("`source_invoice spend (original currency)`").alias("source_invoice spend (original currency)"), 
        col("`source_mg code`").alias("source_mg code"), 
        col("`source_mg code description`").alias("source_mg code description"), 
        col("`source_procurement channel code - procurement channel code`")\
          .alias("source_procurement channel code - procurement channel code"), 
        col("`source_requester - user/buyer name responsible for spend`")\
          .alias("source_requester - user/buyer name responsible for spend"), 
        col("`source_sap_vendor description`").alias("source_sap_vendor description"), 
        col("`source_vendor - cc1 - indicator`").alias("source_vendor - cc1 - indicator"), 
        col("`source_vendor - cc1 - type`").alias("source_vendor - cc1 - type"), 
        col("`source_vendor - cc2 - women owned small bus`").alias("source_vendor - cc2 - women owned small bus"), 
        col("`source_vendor - cc3 - svc disabled vet owned sb`")\
          .alias("source_vendor - cc3 - svc disabled vet owned sb"), 
        col("`source_vendor - cc3 - veteran owned small bus`").alias("source_vendor - cc3 - veteran owned small bus"), 
        col("`source_vendor - cc4 - hubzone small business`").alias("source_vendor - cc4 - hubzone small business"), 
        col("`source_vendor - cc4 - obs-foreign supplier`").alias("source_vendor - cc4 - obs-foreign supplier"), 
        col("`source_vendor - cc5 - obs-large business`").alias("source_vendor - cc5 - obs-large business"), 
        col("`source_vendor - cc5 - small business`").alias("source_vendor - cc5 - small business"), 
        col("`source_vendor - vendor (enriched)`").alias("source_vendor - vendor (enriched)"), 
        col("`source_vendor - vendor global ultimate parent (enriched)`")\
          .alias("source_vendor - vendor global ultimate parent (enriched)"), 
        col("`source_vendor - vendor global ultimate parent (enriched) old`")\
          .alias("source_vendor - vendor global ultimate parent (enriched) old"), 
        col("`source_vendor - vendor global ultimate parent (enriched) temp`")\
          .alias("source_vendor - vendor global ultimate parent (enriched) temp"), 
        col("source_vendor_number"), 
        col("`source_wbs - accounting treatment`").alias("source_wbs - accounting treatment"), 
        col("`source_wbs - description`").alias("source_wbs - description"), 
        col("`source_wbs - product name`").alias("source_wbs - product name"), 
        col("`tableau display category`").alias("tableau display category"), 
        col("`tableau display material group`").alias("tableau display material group"), 
        col("`tableau display mega category`").alias("tableau display mega category"), 
        col("`tableau display meta category`").alias("tableau display meta category"), 
        col("`vendor country`").alias("vendor country"), 
        col("`vendor country code`").alias("vendor country code")
    )
