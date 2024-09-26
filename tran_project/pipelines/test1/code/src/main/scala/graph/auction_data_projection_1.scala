package graph

import io.prophecy.libs._
import udfs.PipelineInitCode._
import udfs.UDFs._
import udfs.ColumnFunctions._
import config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object auction_data_projection_1 {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.select(
      col("date_time"),
      col("auction_id_64"),
      col("seller_member_id"),
      col("buyer_member_id"),
      col("width"),
      col("height"),
      col("publisher_id"),
      col("site_id"),
      col("tag_id"),
      when(col("gender") === lit("u"), lit(null).cast(StringType))
        .otherwise(col("gender"))
        .as("gender"),
      col("geo_country"),
      when(col("inventory_source_id") === lit(0), lit(null).cast(IntegerType))
        .otherwise(col("inventory_source_id"))
        .as("inventory_source_id"),
      col("imp_type"),
      col("is_dw"),
      col("bidder_id"),
      when(col("sampling_pct") === lit(1.0d), lit(null).cast(DoubleType))
        .otherwise(col("sampling_pct"))
        .as("sampling_pct"),
      when(col("seller_revenue") === lit(0.0d), lit(null).cast(DoubleType))
        .otherwise(col("seller_revenue"))
        .as("seller_revenue"),
      when(col("buyer_spend") === lit(0.0d), lit(null).cast(DoubleType))
        .otherwise(col("buyer_spend"))
        .as("buyer_spend"),
      col("cleared_direct"),
      when(col("creative_overage_fees") === lit(0.0d),
           lit(null).cast(DoubleType)
      ).otherwise(col("creative_overage_fees")).as("creative_overage_fees"),
      when(col("auction_service_fees") === lit(0.0d),
           lit(null).cast(DoubleType)
      ).otherwise(col("auction_service_fees")).as("auction_service_fees"),
      when(col("clear_fees") === lit(0.0d), lit(null).cast(DoubleType))
        .otherwise(col("clear_fees"))
        .as("clear_fees"),
      when(col("discrepancy_allowance") === lit(0.0d),
           lit(null).cast(DoubleType)
      ).otherwise(col("discrepancy_allowance")).as("discrepancy_allowance"),
      when(col("forex_allowance") === lit(0.0d), lit(null).cast(DoubleType))
        .otherwise(col("forex_allowance"))
        .as("forex_allowance"),
      when(col("auction_service_deduction") === lit(0.0d),
           lit(null).cast(DoubleType)
      ).otherwise(col("auction_service_deduction"))
        .as("auction_service_deduction"),
      when(col("content_category_id") === lit(0), lit(null).cast(IntegerType))
        .otherwise(col("content_category_id"))
        .as("content_category_id"),
      col("datacenter_id"),
      col("imp_bid_on"),
      col("ecp"),
      col("eap"),
      when(col("buyer_currency") === lit("USD"), lit(null).cast(StringType))
        .otherwise(col("buyer_currency"))
        .as("buyer_currency"),
      when(col("buyer_spend_buyer_currency") === lit(0.0d),
           lit(null).cast(DoubleType)
      ).otherwise(col("buyer_spend_buyer_currency"))
        .as("buyer_spend_buyer_currency"),
      when(col("seller_currency") === lit("USD"), lit(null).cast(StringType))
        .otherwise(col("seller_currency"))
        .as("seller_currency"),
      when(col("seller_revenue_seller_currency") === lit(0.0d),
           lit(null).cast(DoubleType)
      ).otherwise(col("seller_revenue_seller_currency"))
        .as("seller_revenue_seller_currency"),
      when(col("vp_expose_pubs") === lit(1), lit(null).cast(IntegerType))
        .otherwise(col("vp_expose_pubs"))
        .as("vp_expose_pubs"),
      when(col("seller_deduction") === lit(0.0d), lit(null).cast(DoubleType))
        .otherwise(col("seller_deduction"))
        .as("seller_deduction"),
      col("supply_type"),
      when(col("is_delivered") === lit(0), lit(null).cast(IntegerType))
        .otherwise(col("is_delivered"))
        .as("is_delivered"),
      col("buyer_bid_bucket"),
      when(col("device_id") === lit(0), lit(null).cast(IntegerType))
        .otherwise(col("device_id"))
        .as("device_id"),
      when(col("user_id_64") === lit(0), lit(null).cast(LongType))
        .otherwise(col("user_id_64"))
        .as("user_id_64"),
      when(col("cookie_age") === lit(0), lit(null).cast(IntegerType))
        .otherwise(col("cookie_age"))
        .as("cookie_age"),
      when(col("ip_address") === lit(""), lit(null).cast(StringType))
        .otherwise(col("ip_address"))
        .as("ip_address"),
      col("imp_blacklist_or_fraud"),
      when(col("site_domain") === lit("---"), lit(null).cast(StringType))
        .otherwise(col("site_domain"))
        .as("site_domain"),
      when(col("view_measurable") === lit(0), lit(null).cast(IntegerType))
        .otherwise(col("view_measurable"))
        .as("view_measurable"),
      when(col("viewable") === lit(0), lit(null).cast(IntegerType))
        .otherwise(col("viewable"))
        .as("viewable"),
      col("call_type"),
      col("deal_id"),
      when(col("application_id") === lit("---"), lit(null).cast(StringType))
        .otherwise(col("application_id"))
        .as("application_id"),
      col("imp_date_time"),
      col("media_type"),
      col("pub_rule_id"),
      col("venue_id"),
      col("buyer_charges"),
      col("seller_charges"),
      col("buyer_bid"),
      col("preempt_ip_address"),
      col("seller_transaction_def"),
      col("buyer_transaction_def"),
      when(col("is_imp_rejecter_applied") === lit(false),
           lit(null).cast(BooleanType)
      ).otherwise(col("is_imp_rejecter_applied")).as("is_imp_rejecter_applied"),
      col("imp_rejecter_do_auction"),
      col("audit_type"),
      col("browser"),
      when(col("device_type") === lit(0), lit(null).cast(IntegerType))
        .otherwise(col("device_type"))
        .as("device_type"),
      when(col("geo_region") === lit("--"), lit(null).cast(StringType))
        .otherwise(col("geo_region"))
        .as("geo_region"),
      col("language"),
      col("operating_system"),
      when(col("operating_system_family_id") === lit(1),
           lit(null).cast(IntegerType)
      ).otherwise(col("operating_system_family_id"))
        .as("operating_system_family_id"),
      col("allowed_media_types"),
      when(col("imp_biddable") === lit(false), lit(null).cast(BooleanType))
        .otherwise(col("imp_biddable"))
        .as("imp_biddable"),
      col("imp_ignored"),
      col("user_group_id"),
      col("inventory_url_id"),
      col("vp_expose_domains"),
      col("visibility_profile_id"),
      col("is_exclusive"),
      col("truncate_ip"),
      col("creative_id"),
      col("buyer_exchange_rate"),
      col("vp_expose_tag"),
      col("view_result"),
      col("age"),
      col("brand_id"),
      col("carrier_id"),
      col("city"),
      col("dma"),
      col("device_unique_id"),
      col("latitude"),
      col("longitude"),
      when(col("postal") === lit("---"), lit(null).cast(StringType))
        .otherwise(col("postal"))
        .as("postal"),
      when(col("sdk_version") === lit("---"), lit(null).cast(StringType))
        .otherwise(col("sdk_version"))
        .as("sdk_version"),
      when(col("pricing_media_type") === lit(0), lit(null).cast(IntegerType))
        .otherwise(col("pricing_media_type"))
        .as("pricing_media_type"),
      when(col("traffic_source_code") === lit("---"),
           lit(null).cast(StringType)
      ).otherwise(col("traffic_source_code")).as("traffic_source_code"),
      when(col("is_prebid") === lit(false), lit(null).cast(BooleanType))
        .otherwise(col("is_prebid"))
        .as("is_prebid"),
      col("is_unit_of_buyer_trx"),
      col("is_unit_of_seller_trx"),
      when(col("two_phase_reduction_applied") === lit(false),
           lit(null).cast(BooleanType)
      ).otherwise(col("two_phase_reduction_applied"))
        .as("two_phase_reduction_applied"),
      when(col("region_id") === lit(0), lit(null).cast(IntegerType))
        .otherwise(col("region_id"))
        .as("region_id"),
      col("personal_data"),
      col("anonymized_user_info"),
      when(col("gdpr_consent_cookie") === lit(""), lit(null).cast(StringType))
        .otherwise(col("gdpr_consent_cookie"))
        .as("gdpr_consent_cookie"),
      col("external_creative_id"),
      when(col("subject_to_gdpr") === lit(false), lit(null).cast(BooleanType))
        .otherwise(col("subject_to_gdpr"))
        .as("subject_to_gdpr"),
      col("fx_rate_snapshot_id"),
      col("view_detection_enabled"),
      col("seller_exchange_rate"),
      when(col("browser_code_id") === lit(0), lit(null).cast(IntegerType))
        .otherwise(col("browser_code_id"))
        .as("browser_code_id"),
      when(col("is_prebid_server_included") === lit(0),
           lit(null).cast(IntegerType)
      ).otherwise(col("is_prebid_server_included"))
        .as("is_prebid_server_included"),
      col("bidder_seat_id"),
      when(col("default_referrer_url") === lit(""), lit(null).cast(StringType))
        .otherwise(col("default_referrer_url"))
        .as("default_referrer_url"),
      when(col("pred_info") === lit(0), lit(null).cast(IntegerType))
        .otherwise(col("pred_info"))
        .as("pred_info"),
      when(col("curated_deal_id") === lit(0), lit(null).cast(IntegerType))
        .otherwise(col("curated_deal_id"))
        .as("curated_deal_id"),
      when(col("deal_type") === lit(0), lit(null).cast(IntegerType))
        .otherwise(col("deal_type"))
        .as("deal_type"),
      when(col("primary_height") === lit(300), lit(null).cast(IntegerType))
        .otherwise(col("primary_height"))
        .as("primary_height"),
      when(col("primary_width") === lit(50), lit(null).cast(IntegerType))
        .otherwise(col("primary_width"))
        .as("primary_width"),
      col("curator_member_id"),
      col("instance_id"),
      col("hb_source"),
      when(col("from_imps_seen") === lit(true), lit(null).cast(BooleanType))
        .otherwise(col("from_imps_seen"))
        .as("from_imps_seen"),
      col("external_campaign_id"),
      col("ss_native_assembly_enabled"),
      col("uid_source"),
      col("video_context"),
      when(size(col("personal_identifiers")) === 0, null)
        .otherwise(col("personal_identifiers"))
        .as("personal_identifiers"),
      when(size(col("personal_identifiers_experimental")) === 0, null)
        .otherwise(col("personal_identifiers_experimental"))
        .as("personal_identifiers_experimental"),
      col("user_tz_offset"),
      col("external_bidrequest_id"),
      col("external_bidrequest_imp_id"),
      when(col("ym_floor_id") === lit(0), lit(null).cast(IntegerType))
        .otherwise(col("ym_floor_id"))
        .as("ym_floor_id"),
      when(col("ym_bias_id") === lit(0), lit(null).cast(IntegerType))
        .otherwise(col("ym_bias_id"))
        .as("ym_bias_id"),
      col("openrtb_req_subdomain")
    )

}
