from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Union_74_variable1(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("`source_vendor - vendor global ultimate parent (enriched) temp`")\
          .alias("_source_vendor - vendor global ultimate parent (enriched) temp"), 
        col("`source_vendor - vendor (enriched)`").alias("_source_vendor - vendor (enriched)"), 
        col("`source_mg code description`").alias("_source_mg code description"), 
        col("`source_vendor - cc1 - type`").alias("_source_vendor - cc1 - type"), 
        col("`Sum of Commitment Amount`").alias("_Sum of Commitment Amount"), 
        col("`source_amgen region/country - world sub-region_temp`")\
          .alias("_source_amgen region/country - world sub-region_temp"), 
        col("`hyperion function level 2`").alias("_hyperion function level 2"), 
        col("`po line number`").alias("_po line number"), 
        col("`source_invoice spend (budget fx rate)`").alias("_source_invoice spend (budget fx rate)"), 
        col("`source_accounting date`").alias("_source_accounting date"), 
        col("`source_vendor - cc3 - veteran owned small bus`").alias("_source_vendor - cc3 - veteran owned small bus"), 
        col("`vendor country`").alias("_vendor country"), 
        col("`company code name`").alias("_company code name"), 
        col("`source_procurement channel code - procurement channel code`")\
          .alias("_source_procurement channel code - procurement channel code"), 
        col("`Regrouped Level 4`").alias("_Regrouped Level 4"), 
        col("GL").alias("_GL"), 
        col("`tableau display mega category`").alias("_tableau display mega category"), 
        col("`hyperion sub mc2`").alias("_hyperion sub mc2"), 
        col("`source_invoice number`").alias("_source_invoice number"), 
        col("`source_amgen region/country - world sub-region`").alias("_source_amgen region/country - world sub-region"), 
        col("`source_invoice quantity`").alias("_source_invoice quantity"), 
        col("`source_accounting date - year`").alias("_source_accounting date - year"), 
        col("`po description`").alias("_po description"), 
        col("`fit supplier diversity flag`").alias("_fit supplier diversity flag"), 
        col("`source_mg code`").alias("_source_mg code"), 
        col("`hyperion function level 3`").alias("_hyperion function level 3"), 
        col("`source_invoice spend (original currency)`").alias("_source_invoice spend (original currency)"), 
        col("`source_amgen region/country - world sub-region Old`")\
          .alias("_source_amgen region/country - world sub-region Old"), 
        col("`source_amgen region/country - country`").alias("_source_amgen region/country - country"), 
        col("`tableau display category`").alias("_tableau display category"), 
        col("`hyperion sub mc`").alias("_hyperion sub mc"), 
        col("`source_requester - user/buyer name responsible for spend`")\
          .alias("_source_requester - user/buyer name responsible for spend"), 
        col("`source_amgen region/country - sap company code`").alias("_source_amgen region/country - sap company code"), 
        col("`source_sap_vendor description`").alias("_source_sap_vendor description"), 
        col("`Exclusion Type`").alias("_Exclusion Type"), 
        col("`Planning Account`").alias("_Planning Account"), 
        col("`source_vendor - cc3 - svc disabled vet owned sb`")\
          .alias("_source_vendor - cc3 - svc disabled vet owned sb"), 
        col("`tableau display meta category`").alias("_tableau display meta category"), 
        col("`source_vendor - cc5 - obs-large business`").alias("_source_vendor - cc5 - obs-large business"), 
        col("Site").alias("_Site"), 
        col("`source_cost center`").alias("_source_cost center"), 
        col("`hyperion sub mc3`").alias("_hyperion sub mc3"), 
        col("`source_accounting date - quarter`").alias("_source_accounting date - quarter"), 
        col("`hyperion cc_function_mapping`").alias("_hyperion cc_function_mapping"), 
        col("Country_match_flag").alias("_Country_match_flag"), 
        col("`vendor country code`").alias("_vendor country code"), 
        col("`source_wbs - description`").alias("_source_wbs - description"), 
        col("`source_wbs - product name`").alias("_source_wbs - product name"), 
        col("`hyperion function level 4`").alias("_hyperion function level 4"), 
        col("`source_gl number`").alias("_source_gl number"), 
        col("`source_vendor - cc4 - obs-foreign supplier`").alias("_source_vendor - cc4 - obs-foreign supplier"), 
        col("`source_vendor - cc1 - indicator`").alias("_source_vendor - cc1 - indicator"), 
        col("`GL Account Description Updated`").alias("_GL Account Description Updated"), 
        col("`source_wbs - accounting treatment`").alias("_source_wbs - accounting treatment"), 
        col("`invoice linenumber`").alias("_invoice linenumber"), 
        col("`source_invoice spend (actual fx rate)`").alias("_source_invoice spend (actual fx rate)"), 
        col("source_vendor_number").alias("_source_vendor_number"), 
        col("`hyperion function description`").alias("_hyperion function description"), 
        col("`SUB GROUP`").alias("_SUB GROUP"), 
        col("`Gl Account`").alias("_Gl Account"), 
        col("`Mega Category Old`").alias("_Mega Category Old"), 
        col("`source_vendor - vendor global ultimate parent (enriched) old`")\
          .alias("_source_vendor - vendor global ultimate parent (enriched) old"), 
        col("`company code number`").alias("_company code number"), 
        col("`po id`").alias("_po id"), 
        col("`hyperion mc`").alias("_hyperion mc"), 
        col(
            "`Gl Account (# only)`"
          )\
          .alias(
          "_Gl Account (# only)"
        ), 
        col("`source_vendor - vendor global ultimate parent (enriched)`")\
          .alias("_source_vendor - vendor global ultimate parent (enriched)"), 
        col("`Category Old`").alias("_Category Old"), 
        col("`hyperion cc_function_descr`").alias("_hyperion cc_function_descr"), 
        col("`Country Old`").alias("_Country Old"), 
        col("`hyperion evp`").alias("_hyperion evp"), 
        col("`source_amgen region/country - world region`").alias("_source_amgen region/country - world region"), 
        col("`tableau display material group`").alias("_tableau display material group"), 
        col("`source_vendor - cc2 - women owned small bus`").alias("_source_vendor - cc2 - women owned small bus"), 
        col("`source_vendor - cc5 - small business`").alias("_source_vendor - cc5 - small business"), 
        col("`sap/concur source`").alias("_sap/concur source"), 
        col("`source_cost center number`").alias("_source_cost center number"), 
        col("`source_accounting date - month`").alias("_source_accounting date - month"), 
        col("`hyperion hlmc`").alias("_hyperion hlmc"), 
        col("ParentVendor_match_flag").alias("_ParentVendor_match_flag"), 
        col("`Country Short`").alias("_Country Short"), 
        col("Category").alias("_Category"), 
        col("`hyperion cc description`").alias("_hyperion cc description"), 
        col("`source_vendor - cc4 - hubzone small business`").alias("_source_vendor - cc4 - hubzone small business")
    )
