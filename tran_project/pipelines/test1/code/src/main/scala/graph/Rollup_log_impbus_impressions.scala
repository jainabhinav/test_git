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

object Rollup_log_impbus_impressions {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.groupBy(col("auction_id_64").cast(LongType).as("auction_id_64"))
      .agg(
        last(col("date_time")).cast(LongType).as("date_time"),
        last(col("user_id_64")).cast(LongType).as("user_id_64"),
        last(col("tag_id")).cast(IntegerType).as("tag_id"),
        last(col("ip_address")).as("ip_address"),
        last(col("venue_id")).cast(IntegerType).as("venue_id"),
        last(col("site_domain")).as("site_domain"),
        last(col("width")).cast(IntegerType).as("width"),
        last(col("height")).cast(IntegerType).as("height"),
        last(col("geo_country")).as("geo_country"),
        last(col("geo_region")).as("geo_region"),
        last(col("gender")).as("gender"),
        last(col("age")).cast(IntegerType).as("age"),
        last(col("bidder_id")).cast(IntegerType).as("bidder_id"),
        last(col("seller_member_id")).cast(IntegerType).as("seller_member_id"),
        last(col("buyer_member_id")).cast(IntegerType).as("buyer_member_id"),
        last(col("creative_id")).cast(IntegerType).as("creative_id"),
        last(col("imp_blacklist_or_fraud"))
          .cast(IntegerType)
          .as("imp_blacklist_or_fraud"),
        last(col("imp_bid_on")).cast(IntegerType).as("imp_bid_on"),
        last(col("buyer_bid")).as("buyer_bid"),
        max(col("buyer_spend")).as("buyer_spend"),
        max(col("seller_revenue")).as("seller_revenue"),
        last(col("num_of_bids")).cast(IntegerType).as("num_of_bids"),
        last(col("ecp")).as("ecp"),
        last(col("reserve_price")).as("reserve_price"),
        last(col("inv_code")).as("inv_code"),
        last(col("call_type")).as("call_type"),
        last(col("inventory_source_id"))
          .cast(IntegerType)
          .as("inventory_source_id"),
        last(col("cookie_age")).cast(IntegerType).as("cookie_age"),
        last(col("brand_id")).cast(IntegerType).as("brand_id"),
        last(col("cleared_direct")).cast(IntegerType).as("cleared_direct"),
        last(col("forex_allowance")).as("forex_allowance"),
        last(col("fold_position")).cast(IntegerType).as("fold_position"),
        last(col("external_inv_id")).cast(IntegerType).as("external_inv_id"),
        last(col("imp_type")).cast(IntegerType).as("imp_type"),
        last(col("is_delivered")).cast(IntegerType).as("is_delivered"),
        last(col("is_dw")).cast(IntegerType).as("is_dw"),
        last(col("publisher_id")).cast(IntegerType).as("publisher_id"),
        last(col("site_id")).cast(IntegerType).as("site_id"),
        last(col("content_category_id"))
          .cast(IntegerType)
          .as("content_category_id"),
        last(col("datacenter_id")).cast(IntegerType).as("datacenter_id"),
        last(col("eap")).as("eap"),
        last(col("user_tz_offset")).cast(IntegerType).as("user_tz_offset"),
        last(col("user_group_id")).cast(IntegerType).as("user_group_id"),
        last(col("pub_rule_id")).cast(IntegerType).as("pub_rule_id"),
        last(col("media_type")).cast(IntegerType).as("media_type"),
        last(col("operating_system")).cast(IntegerType).as("operating_system"),
        last(col("browser")).cast(IntegerType).as("browser"),
        last(col("language")).cast(IntegerType).as("language"),
        last(col("application_id")).as("application_id"),
        last(col("user_locale")).as("user_locale"),
        last(col("inventory_url_id")).cast(IntegerType).as("inventory_url_id"),
        last(col("audit_type")).cast(IntegerType).as("audit_type"),
        last(col("shadow_price")).as("shadow_price"),
        last(col("impbus_id")).cast(IntegerType).as("impbus_id"),
        last(col("buyer_currency")).as("buyer_currency"),
        last(col("buyer_exchange_rate")).as("buyer_exchange_rate"),
        last(col("seller_currency")).as("seller_currency"),
        last(col("seller_exchange_rate")).as("seller_exchange_rate"),
        last(col("vp_expose_domains"))
          .cast(IntegerType)
          .as("vp_expose_domains"),
        last(col("vp_expose_categories"))
          .cast(IntegerType)
          .as("vp_expose_categories"),
        last(col("vp_expose_pubs")).cast(IntegerType).as("vp_expose_pubs"),
        last(col("vp_expose_tag")).cast(IntegerType).as("vp_expose_tag"),
        last(col("is_exclusive")).cast(IntegerType).as("is_exclusive"),
        last(col("bidder_instance_id"))
          .cast(IntegerType)
          .as("bidder_instance_id"),
        last(col("visibility_profile_id"))
          .cast(IntegerType)
          .as("visibility_profile_id"),
        last(col("truncate_ip")).cast(IntegerType).as("truncate_ip"),
        last(col("device_id")).cast(IntegerType).as("device_id"),
        last(col("carrier_id")).cast(IntegerType).as("carrier_id"),
        last(col("creative_audit_status"))
          .cast(IntegerType)
          .as("creative_audit_status"),
        last(col("is_creative_hosted"))
          .cast(IntegerType)
          .as("is_creative_hosted"),
        last(col("city")).cast(IntegerType).as("city"),
        last(col("latitude")).as("latitude"),
        last(col("longitude")).as("longitude"),
        last(col("device_unique_id")).as("device_unique_id"),
        last(col("supply_type")).cast(IntegerType).as("supply_type"),
        last(col("is_toolbar")).cast(IntegerType).as("is_toolbar"),
        last(col("deal_id")).cast(IntegerType).as("deal_id"),
        last(col("vp_bitmap")).cast(LongType).as("vp_bitmap"),
        last(col("ttl")).cast(IntegerType).as("ttl"),
        last(col("view_detection_enabled"))
          .cast(IntegerType)
          .as("view_detection_enabled"),
        last(col("ozone_id")).cast(IntegerType).as("ozone_id"),
        last(col("is_performance")).cast(IntegerType).as("is_performance"),
        last(col("sdk_version")).as("sdk_version"),
        last(col("inventory_session_frequency"))
          .cast(IntegerType)
          .as("inventory_session_frequency"),
        last(col("bid_price_type")).cast(IntegerType).as("bid_price_type"),
        last(col("device_type")).cast(IntegerType).as("device_type"),
        last(col("dma")).cast(IntegerType).as("dma"),
        last(col("postal")).as("postal"),
        last(col("package_id")).cast(IntegerType).as("package_id"),
        last(col("spend_protection")).cast(IntegerType).as("spend_protection"),
        last(col("is_secure")).cast(IntegerType).as("is_secure"),
        last(col("estimated_view_rate")).as("estimated_view_rate"),
        last(col("external_request_id")).as("external_request_id"),
        last(col("viewdef_definition_id_buyer_member"))
          .cast(IntegerType)
          .as("viewdef_definition_id_buyer_member"),
        last(col("spend_protection_pixel_id"))
          .cast(IntegerType)
          .as("spend_protection_pixel_id"),
        last(col("external_uid")).as("external_uid"),
        last(col("request_uuid")).as("request_uuid"),
        last(col("mobile_app_instance_id"))
          .cast(IntegerType)
          .as("mobile_app_instance_id"),
        last(col("traffic_source_code")).as("traffic_source_code"),
        last(col("stitch_group_id")).as("stitch_group_id"),
        last(col("deal_type")).cast(IntegerType).as("deal_type"),
        last(col("ym_floor_id")).cast(IntegerType).as("ym_floor_id"),
        last(col("ym_bias_id")).cast(IntegerType).as("ym_bias_id"),
        last(col("estimated_view_rate_over_total"))
          .as("estimated_view_rate_over_total"),
        last(col("device_make_id")).cast(IntegerType).as("device_make_id"),
        last(col("operating_system_family_id"))
          .cast(IntegerType)
          .as("operating_system_family_id"),
        last(col("tag_sizes")).as("tag_sizes"),
        last(col("seller_transaction_def")).as("seller_transaction_def"),
        last(col("buyer_transaction_def")).as("buyer_transaction_def"),
        last(col("predicted_video_view_info")).as("predicted_video_view_info"),
        last(col("auction_url")).as("auction_url"),
        last(col("allowed_media_types")).as("allowed_media_types"),
        last(col("is_imp_rejecter_applied"))
          .cast(BooleanType)
          .as("is_imp_rejecter_applied"),
        last(col("imp_rejecter_do_auction"))
          .cast(BooleanType)
          .as("imp_rejecter_do_auction"),
        last(col("geo_location")).as("geo_location"),
        last(col("seller_bid_currency_conversion_rate"))
          .as("seller_bid_currency_conversion_rate"),
        last(col("seller_bid_currency_code")).as("seller_bid_currency_code"),
        last(col("is_prebid")).cast(BooleanType).as("is_prebid"),
        last(col("default_referrer_url")).as("default_referrer_url"),
        last(col("engagement_rates")).as("engagement_rates"),
        last(col("fx_rate_snapshot_id"))
          .cast(IntegerType)
          .as("fx_rate_snapshot_id"),
        last(col("payment_type")).cast(IntegerType).as("payment_type"),
        last(col("apply_cost_on_default"))
          .cast(IntegerType)
          .as("apply_cost_on_default"),
        last(col("media_buy_cost")).as("media_buy_cost"),
        last(col("media_buy_rev_share_pct")).as("media_buy_rev_share_pct"),
        last(col("auction_duration_ms"))
          .cast(IntegerType)
          .as("auction_duration_ms"),
        last(col("expected_events")).cast(IntegerType).as("expected_events"),
        last(col("anonymized_user_info")).as("anonymized_user_info"),
        last(col("region_id")).cast(IntegerType).as("region_id"),
        last(col("media_company_id")).cast(IntegerType).as("media_company_id"),
        last(col("gdpr_consent_cookie")).as("gdpr_consent_cookie"),
        last(col("subject_to_gdpr")).cast(BooleanType).as("subject_to_gdpr"),
        last(col("browser_code_id")).cast(IntegerType).as("browser_code_id"),
        last(col("is_prebid_server_included"))
          .cast(IntegerType)
          .as("is_prebid_server_included"),
        last(col("seat_id")).cast(IntegerType).as("seat_id"),
        last(col("uid_source")).cast(IntegerType).as("uid_source"),
        last(col("is_whiteops_scanned"))
          .cast(BooleanType)
          .as("is_whiteops_scanned"),
        last(col("pred_info")).cast(IntegerType).as("pred_info"),
        last(col("crossdevice_groups")).as("crossdevice_groups"),
        last(col("is_amp")).cast(BooleanType).as("is_amp"),
        last(col("hb_source")).cast(IntegerType).as("hb_source"),
        last(col("external_campaign_id")).as("external_campaign_id"),
        last(col("log_product_ads")).as("log_product_ads"),
        last(col("ss_native_assembly_enabled"))
          .cast(BooleanType)
          .as("ss_native_assembly_enabled"),
        last(col("emp")).as("emp"),
        last(col("personal_identifiers")).as("personal_identifiers"),
        last(col("personal_identifiers_experimental"))
          .as("personal_identifiers_experimental"),
        last(col("postal_code_ext_id"))
          .cast(IntegerType)
          .as("postal_code_ext_id"),
        last(col("hashed_ip")).as("hashed_ip"),
        last(col("external_deal_code")).as("external_deal_code"),
        last(col("creative_duration"))
          .cast(IntegerType)
          .as("creative_duration"),
        last(col("openrtb_req_subdomain")).as("openrtb_req_subdomain"),
        last(col("creative_media_subtype_id"))
          .cast(IntegerType)
          .as("creative_media_subtype_id"),
        last(col("is_private_auction"))
          .cast(BooleanType)
          .as("is_private_auction"),
        last(col("private_auction_eligible"))
          .cast(BooleanType)
          .as("private_auction_eligible"),
        last(col("client_request_id")).as("client_request_id"),
        last(col("chrome_traffic_label"))
          .cast(IntegerType)
          .as("chrome_traffic_label")
      )

}
