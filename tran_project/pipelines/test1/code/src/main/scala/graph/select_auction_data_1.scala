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

object select_auction_data_1 {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.select(
      col("date_time"),
      col("auction_id_64"),
      col("bids"),
      col("dw_views"),
      col("auction_events"),
      impressions(context).as("impressions"),
      col("pricings"),
      col("imps_seen"),
      col("preempts"),
      col("spend_protections"),
      col("impbus_views"),
      col("stage_seen_out"),
      col("dw_imps_out"),
      col("quarantine_reasons")
    )

  def impressions(context: Context) = {
    val spark  = context.spark
    val Config = context.config
    struct(
      col("impressions.date_time").as("date_time"),
      col("impressions.auction_id_64").as("auction_id_64"),
      col("impressions.user_id_64").as("user_id_64"),
      col("impressions.tag_id").as("tag_id"),
      col("impressions.ip_address").as("ip_address"),
      col("impressions.venue_id").as("venue_id"),
      col("impressions.site_domain").as("site_domain"),
      col("impressions.width").as("width"),
      col("impressions.height").as("height"),
      col("impressions.geo_country").as("geo_country"),
      col("impressions.geo_region").as("geo_region"),
      col("impressions.gender").as("gender"),
      col("impressions.age").as("age"),
      col("impressions.bidder_id").as("bidder_id"),
      col("impressions.seller_member_id").as("seller_member_id"),
      col("impressions.buyer_member_id").as("buyer_member_id"),
      col("impressions.creative_id").as("creative_id"),
      col("impressions.imp_blacklist_or_fraud").as("imp_blacklist_or_fraud"),
      col("impressions.imp_bid_on").as("imp_bid_on"),
      col("impressions.buyer_bid").as("buyer_bid"),
      col("impressions.buyer_spend").as("buyer_spend"),
      col("impressions.seller_revenue").as("seller_revenue"),
      col("impressions.num_of_bids").as("num_of_bids"),
      col("impressions.ecp").as("ecp"),
      col("impressions.reserve_price").as("reserve_price"),
      when(col("impressions.inv_code") === lit("---"),
           lit(null).cast(StringType)
      ).otherwise(col("impressions.inv_code")).as("inv_code"),
      col("impressions.call_type").as("call_type"),
      when(col("impressions.inventory_source_id") === lit(0),
           lit(null).cast(IntegerType)
      ).otherwise(col("impressions.inventory_source_id"))
        .as("inventory_source_id"),
      col("impressions.cookie_age").as("cookie_age"),
      col("impressions.brand_id").as("brand_id"),
      col("impressions.cleared_direct").as("cleared_direct"),
      col("impressions.forex_allowance").as("forex_allowance"),
      col("impressions.fold_position").as("fold_position"),
      col("impressions.external_inv_id").as("external_inv_id"),
      col("impressions.imp_type").as("imp_type"),
      col("impressions.is_delivered").as("is_delivered"),
      col("impressions.is_dw").as("is_dw"),
      col("impressions.publisher_id").as("publisher_id"),
      col("impressions.site_id").as("site_id"),
      col("impressions.content_category_id").as("content_category_id"),
      col("impressions.datacenter_id").as("datacenter_id"),
      col("impressions.eap").as("eap"),
      col("impressions.user_tz_offset").as("user_tz_offset"),
      col("impressions.user_group_id").as("user_group_id"),
      col("impressions.pub_rule_id").as("pub_rule_id"),
      col("impressions.media_type").as("media_type"),
      col("impressions.operating_system").as("operating_system"),
      col("impressions.browser").as("browser"),
      col("impressions.language").as("language"),
      when(col("impressions.application_id") === lit("---"),
           lit(null).cast(StringType)
      ).otherwise(col("impressions.application_id")).as("application_id"),
      when(col("impressions.user_locale") === lit("---"),
           lit(null).cast(StringType)
      ).otherwise(col("impressions.user_locale")).as("user_locale"),
      col("impressions.inventory_url_id").as("inventory_url_id"),
      col("impressions.audit_type").as("audit_type"),
      col("impressions.shadow_price").as("shadow_price"),
      col("impressions.impbus_id").as("impbus_id"),
      col("impressions.buyer_currency").as("buyer_currency"),
      col("impressions.buyer_exchange_rate").as("buyer_exchange_rate"),
      col("impressions.seller_currency").as("seller_currency"),
      col("impressions.seller_exchange_rate").as("seller_exchange_rate"),
      col("impressions.vp_expose_domains").as("vp_expose_domains"),
      col("impressions.vp_expose_categories").as("vp_expose_categories"),
      col("impressions.vp_expose_pubs").as("vp_expose_pubs"),
      col("impressions.vp_expose_tag").as("vp_expose_tag"),
      col("impressions.is_exclusive").as("is_exclusive"),
      col("impressions.bidder_instance_id").as("bidder_instance_id"),
      col("impressions.visibility_profile_id").as("visibility_profile_id"),
      col("impressions.truncate_ip").as("truncate_ip"),
      col("impressions.device_id").as("device_id"),
      col("impressions.carrier_id").as("carrier_id"),
      col("impressions.creative_audit_status").as("creative_audit_status"),
      col("impressions.is_creative_hosted").as("is_creative_hosted"),
      col("impressions.city").as("city"),
      when(col("impressions.latitude") === lit(""), lit(null).cast(StringType))
        .otherwise(col("impressions.latitude"))
        .as("latitude"),
      when(col("impressions.longitude") === lit(""), lit(null).cast(StringType))
        .otherwise(col("impressions.longitude"))
        .as("longitude"),
      when(col("impressions.device_unique_id") === lit(""),
           lit(null).cast(StringType)
      ).otherwise(col("impressions.device_unique_id")).as("device_unique_id"),
      col("impressions.supply_type").as("supply_type"),
      col("impressions.is_toolbar").as("is_toolbar"),
      col("impressions.deal_id").as("deal_id"),
      col("impressions.vp_bitmap").as("vp_bitmap"),
      col("impressions.ttl").as("ttl"),
      col("impressions.view_detection_enabled").as("view_detection_enabled"),
      when(col("impressions.ozone_id") === lit(0), lit(null).cast(IntegerType))
        .otherwise(col("impressions.ozone_id"))
        .as("ozone_id"),
      when(col("impressions.is_performance") === lit(0),
           lit(null).cast(IntegerType)
      ).otherwise(col("impressions.is_performance")).as("is_performance"),
      col("impressions.sdk_version").as("sdk_version"),
      when(col("impressions.inventory_session_frequency") === lit(0),
           lit(null).cast(IntegerType)
      ).otherwise(col("impressions.inventory_session_frequency"))
        .as("inventory_session_frequency"),
      col("impressions.bid_price_type").as("bid_price_type"),
      col("impressions.device_type").as("device_type"),
      col("impressions.dma").as("dma"),
      col("impressions.postal").as("postal"),
      col("impressions.package_id").as("package_id"),
      when(col("impressions.spend_protection") === lit(0),
           lit(null).cast(IntegerType)
      ).otherwise(col("impressions.spend_protection")).as("spend_protection"),
      col("impressions.is_secure").as("is_secure"),
      col("impressions.estimated_view_rate").as("estimated_view_rate"),
      when(col("impressions.external_request_id") === lit("---"),
           lit(null).cast(StringType)
      ).otherwise(col("impressions.external_request_id"))
        .as("external_request_id"),
      col("impressions.viewdef_definition_id_buyer_member")
        .as("viewdef_definition_id_buyer_member"),
      col("impressions.spend_protection_pixel_id")
        .as("spend_protection_pixel_id"),
      col("impressions.external_uid").as("external_uid"),
      col("impressions.request_uuid").as("request_uuid"),
      col("impressions.mobile_app_instance_id").as("mobile_app_instance_id"),
      when(col("impressions.traffic_source_code") === lit("---"),
           lit(null).cast(StringType)
      ).otherwise(col("impressions.traffic_source_code"))
        .as("traffic_source_code"),
      col("impressions.stitch_group_id").as("stitch_group_id"),
      col("impressions.deal_type").as("deal_type"),
      col("impressions.ym_floor_id").as("ym_floor_id"),
      col("impressions.ym_bias_id").as("ym_bias_id"),
      col("impressions.estimated_view_rate_over_total")
        .as("estimated_view_rate_over_total"),
      col("impressions.device_make_id").as("device_make_id"),
      col("impressions.operating_system_family_id")
        .as("operating_system_family_id"),
      col("impressions.tag_sizes").as("tag_sizes"),
      col("impressions.seller_transaction_def").as("seller_transaction_def"),
      when(allFieldsNull(col("impressions.buyer_transaction_def")), null)
        .otherwise(col("impressions.buyer_transaction_def"))
        .as("buyer_transaction_def"),
      struct(
        col("impressions.predicted_video_view_info.iab_view_rate_over_measured")
          .as("iab_view_rate_over_measured"),
        col("impressions.predicted_video_view_info.iab_view_rate_over_total")
          .as("iab_view_rate_over_total"),
        col(
          "impressions.predicted_video_view_info.predicted_100pv50pd_video_view_rate"
        ).as("predicted_100pv50pd_video_view_rate"),
        col(
          "impressions.predicted_video_view_info.predicted_100pv50pd_video_view_rate_over_total"
        ).as("predicted_100pv50pd_video_view_rate_over_total"),
        when(col(
               "impressions.predicted_video_view_info.video_completion_rate"
             ) === lit(-1),
             lit(null).cast(IntegerType)
        ).otherwise(
            col("impressions.predicted_video_view_info.video_completion_rate")
          )
          .as("video_completion_rate"),
        when(col(
               "impressions.predicted_video_view_info.view_prediction_source"
             ) === lit(0),
             lit(null).cast(IntegerType)
        ).otherwise(
            col("impressions.predicted_video_view_info.view_prediction_source")
          )
          .as("view_prediction_source")
      ).as("predicted_video_view_info"),
      col("impressions.auction_url").as("auction_url"),
      col("impressions.allowed_media_types").as("allowed_media_types"),
      col("impressions.is_imp_rejecter_applied").as("is_imp_rejecter_applied"),
      col("impressions.imp_rejecter_do_auction").as("imp_rejecter_do_auction"),
      when(allFieldsNull(col("impressions.geo_location")), null)
        .otherwise(col("impressions.geo_location"))
        .as("geo_location"),
      col("impressions.seller_bid_currency_conversion_rate")
        .as("seller_bid_currency_conversion_rate"),
      col("impressions.seller_bid_currency_code")
        .as("seller_bid_currency_code"),
      col("impressions.is_prebid").as("is_prebid"),
      col("impressions.default_referrer_url").as("default_referrer_url"),
      when(size(col("impressions.engagement_rates")) === 0, null)
        .otherwise(col("impressions.engagement_rates"))
        .as("engagement_rates"),
      col("impressions.fx_rate_snapshot_id").as("fx_rate_snapshot_id"),
      col("impressions.payment_type").as("payment_type"),
      col("impressions.apply_cost_on_default").as("apply_cost_on_default"),
      col("impressions.media_buy_cost").as("media_buy_cost"),
      col("impressions.media_buy_rev_share_pct").as("media_buy_rev_share_pct"),
      col("impressions.auction_duration_ms").as("auction_duration_ms"),
      col("impressions.expected_events").as("expected_events"),
      when(allFieldsNull(col("impressions.anonymized_user_info")), null)
        .otherwise(col("impressions.anonymized_user_info"))
        .as("anonymized_user_info"),
      col("impressions.region_id").as("region_id"),
      col("impressions.media_company_id").as("media_company_id"),
      col("impressions.gdpr_consent_cookie").as("gdpr_consent_cookie"),
      col("impressions.subject_to_gdpr").as("subject_to_gdpr"),
      col("impressions.browser_code_id").as("browser_code_id"),
      col("impressions.is_prebid_server_included")
        .as("is_prebid_server_included"),
      col("impressions.seat_id").as("seat_id"),
      col("impressions.uid_source").as("uid_source"),
      col("impressions.is_whiteops_scanned").as("is_whiteops_scanned"),
      col("impressions.pred_info").as("pred_info"),
      when(size(col("impressions.crossdevice_groups")) === 0, null)
        .otherwise(col("impressions.crossdevice_groups"))
        .as("crossdevice_groups"),
      col("impressions.is_amp").as("is_amp"),
      col("impressions.hb_source").as("hb_source"),
      col("impressions.external_campaign_id").as("external_campaign_id"),
      col("impressions.log_product_ads").as("log_product_ads"),
      col("impressions.ss_native_assembly_enabled")
        .as("ss_native_assembly_enabled"),
      col("impressions.emp").as("emp"),
      when(allFieldsNull(col("impressions.personal_identifiers")), null)
        .otherwise(col("impressions.personal_identifiers"))
        .as("personal_identifiers"),
      when(allFieldsNull(col("impressions.personal_identifiers_experimental")),
           null
      ).otherwise(col("impressions.personal_identifiers_experimental"))
        .as("personal_identifiers_experimental"),
      col("impressions.postal_code_ext_id").as("postal_code_ext_id"),
      col("impressions.hashed_ip").as("hashed_ip"),
      col("impressions.external_deal_code").as("external_deal_code"),
      col("impressions.creative_duration").as("creative_duration"),
      col("impressions.openrtb_req_subdomain").as("openrtb_req_subdomain"),
      when(col("impressions.creative_media_subtype_id") === lit(0),
           lit(null).cast(IntegerType)
      ).otherwise(col("impressions.creative_media_subtype_id"))
        .as("creative_media_subtype_id"),
      when(col("impressions.is_private_auction") === lit(false),
           lit(null).cast(BooleanType)
      ).otherwise(col("impressions.is_private_auction"))
        .as("is_private_auction"),
      when(col("impressions.private_auction_eligible") === lit(false),
           lit(null).cast(BooleanType)
      ).otherwise(col("impressions.private_auction_eligible"))
        .as("private_auction_eligible"),
      col("impressions.client_request_id").as("client_request_id"),
      when(col("impressions.chrome_traffic_label") === lit(0),
           lit(null).cast(IntegerType)
      ).otherwise(col("impressions.chrome_traffic_label"))
        .as("chrome_traffic_label")
    )
  }

}
