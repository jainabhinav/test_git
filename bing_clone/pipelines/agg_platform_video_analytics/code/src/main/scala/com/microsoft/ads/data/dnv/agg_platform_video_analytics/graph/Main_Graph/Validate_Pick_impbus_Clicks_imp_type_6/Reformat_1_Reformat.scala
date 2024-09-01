package com.microsoft.ads.data.dnv.agg_platform_video_analytics.graph.Main_Graph.Validate_Pick_impbus_Clicks_imp_type_6

import io.prophecy.libs._
import com.microsoft.ads.data.dnv.agg_platform_video_analytics.udfs.PipelineInitCode._
import com.microsoft.ads.data.dnv.agg_platform_video_analytics.udfs.UDFs._
import com.microsoft.ads.data.dnv.agg_platform_video_analytics.udfs.ColumnFunctions._
import com.microsoft.ads.data.dnv.agg_platform_video_analytics.graph.Main_Graph.Validate_Pick_impbus_Clicks_imp_type_6.config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Reformat_1_Reformat {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.select(
      col("date_time").cast(LongType).as("date_time"),
      col("auction_id_64").cast(LongType).as("auction_id_64"),
      col("user_id_64").cast(LongType).as("user_id_64"),
      col("tag_id").cast(IntegerType).as("tag_id"),
      col("venue_id").cast(IntegerType).as("venue_id"),
      col("inventory_source_id").cast(IntegerType).as("inventory_source_id"),
      col("session_frequency").cast(IntegerType).as("session_frequency"),
      col("width").cast(IntegerType).as("width"),
      col("height").cast(IntegerType).as("height"),
      col("geo_country"),
      col("geo_region"),
      col("gender"),
      col("age").cast(IntegerType).as("age"),
      col("seller_member_id").cast(IntegerType).as("seller_member_id"),
      col("buyer_member_id").cast(IntegerType).as("buyer_member_id"),
      col("creative_id").cast(IntegerType).as("creative_id"),
      col("seller_currency"),
      col("buyer_currency"),
      col("advertiser_id").cast(IntegerType).as("advertiser_id"),
      col("campaign_group_id").cast(IntegerType).as("campaign_group_id"),
      col("campaign_id").cast(IntegerType).as("campaign_id"),
      col("creative_freq").cast(IntegerType).as("creative_freq"),
      col("creative_rec").cast(IntegerType).as("creative_rec"),
      col("is_learn").cast(IntegerType).as("is_learn"),
      col("is_remarketing").cast(IntegerType).as("is_remarketing"),
      col("advertiser_frequency").cast(IntegerType).as("advertiser_frequency"),
      col("advertiser_recency").cast(IntegerType).as("advertiser_recency"),
      col("user_group_id").cast(IntegerType).as("user_group_id"),
      col("camp_dp_id").cast(IntegerType).as("camp_dp_id"),
      col("media_buy_id").cast(IntegerType).as("media_buy_id"),
      col("brand_id").cast(IntegerType).as("brand_id"),
      col("is_appnexus_cleared").cast(IntegerType).as("is_appnexus_cleared"),
      col("clear_fees"),
      col("media_buy_rev_share_pct"),
      col("revenue_value"),
      col("pricing_type"),
      col("site_id").cast(IntegerType).as("site_id"),
      col("content_category_id").cast(IntegerType).as("content_category_id"),
      col("fold_position").cast(IntegerType).as("fold_position"),
      col("external_inv_id").cast(IntegerType).as("external_inv_id"),
      col("cadence_modifier"),
      coalesce(col("imp_type").cast(IntegerType), lit(0)).as("imp_type"),
      col("advertiser_currency"),
      col("advertiser_exchange_rate"),
      col("ip_address"),
      col("pub_rule_id").cast(IntegerType).as("pub_rule_id"),
      col("publisher_id").cast(IntegerType).as("publisher_id"),
      col("insertion_order_id").cast(IntegerType).as("insertion_order_id"),
      col("predict_type_rev").cast(IntegerType).as("predict_type_rev"),
      col("predict_type_goal").cast(IntegerType).as("predict_type_goal"),
      col("predict_type_cost").cast(IntegerType).as("predict_type_cost"),
      col("commission_revshare"),
      col("serving_fees_revshare"),
      col("user_tz_offset").cast(IntegerType).as("user_tz_offset"),
      col("media_type").cast(IntegerType).as("media_type"),
      col("operating_system").cast(IntegerType).as("operating_system"),
      col("browser").cast(IntegerType).as("browser"),
      col("language").cast(IntegerType).as("language"),
      col("publisher_currency"),
      col("publisher_exchange_rate"),
      col("site_domain"),
      col("payment_type").cast(IntegerType).as("payment_type"),
      col("revenue_type").cast(IntegerType).as("revenue_type"),
      col("bidder_id").cast(IntegerType).as("bidder_id"),
      col("inv_code"),
      col("application_id"),
      col("is_control").cast(IntegerType).as("is_control"),
      col("vp_expose_domains").cast(IntegerType).as("vp_expose_domains"),
      col("vp_expose_categories").cast(IntegerType).as("vp_expose_categories"),
      col("vp_expose_pubs").cast(IntegerType).as("vp_expose_pubs"),
      col("vp_expose_tag").cast(IntegerType).as("vp_expose_tag"),
      col("vp_expose_age").cast(IntegerType).as("vp_expose_age"),
      col("vp_expose_gender").cast(IntegerType).as("vp_expose_gender"),
      col("inventory_url_id").cast(IntegerType).as("inventory_url_id"),
      col("imp_time"),
      col("is_exclusive").cast(IntegerType).as("is_exclusive"),
      col("truncate_ip").cast(IntegerType).as("truncate_ip"),
      col("datacenter_id").cast(IntegerType).as("datacenter_id"),
      col("device_id").cast(IntegerType).as("device_id"),
      col("carrier_id").cast(IntegerType).as("carrier_id"),
      col("creative_audit_status")
        .cast(IntegerType)
        .as("creative_audit_status"),
      col("is_creative_hosted").cast(IntegerType).as("is_creative_hosted"),
      col("city").cast(IntegerType).as("city"),
      col("latitude"),
      col("longitude"),
      col("device_unique_id"),
      col("targeted_segments"),
      col("supply_type").cast(IntegerType).as("supply_type"),
      col("is_toolbar").cast(IntegerType).as("is_toolbar"),
      col("control_pct"),
      col("deal_id").cast(IntegerType).as("deal_id"),
      col("vp_bitmap").cast(LongType).as("vp_bitmap"),
      col("ozone_id").cast(IntegerType).as("ozone_id"),
      col("is_performance").cast(IntegerType).as("is_performance"),
      col("sdk_version"),
      col("device_type").cast(IntegerType).as("device_type"),
      col("dma").cast(IntegerType).as("dma"),
      col("postal"),
      col("package_id").cast(IntegerType).as("package_id"),
      col("campaign_group_freq").cast(IntegerType).as("campaign_group_freq"),
      col("campaign_group_rec").cast(IntegerType).as("campaign_group_rec"),
      col("insertion_order_freq").cast(IntegerType).as("insertion_order_freq"),
      col("insertion_order_rec").cast(IntegerType).as("insertion_order_rec"),
      col("buyer_gender"),
      col("buyer_age").cast(IntegerType).as("buyer_age"),
      col("targeted_segment_list"),
      col("custom_model_id").cast(IntegerType).as("custom_model_id"),
      col("custom_model_last_modified")
        .cast(LongType)
        .as("custom_model_last_modified"),
      col("custom_model_output_code"),
      col("external_uid"),
      col("request_uuid"),
      col("mobile_app_instance_id")
        .cast(IntegerType)
        .as("mobile_app_instance_id"),
      col("traffic_source_code"),
      col("external_request_id"),
      col("stitch_group_id"),
      col("deal_type").cast(IntegerType).as("deal_type"),
      col("ym_floor_id").cast(IntegerType).as("ym_floor_id"),
      col("ym_bias_id").cast(IntegerType).as("ym_bias_id"),
      col("bid_priority").cast(IntegerType).as("bid_priority"),
      col("viewdef_definition_id")
        .cast(IntegerType)
        .as("viewdef_definition_id"),
      col("view_result").cast(IntegerType).as("view_result"),
      col("view_non_measurable_reason")
        .cast(IntegerType)
        .as("view_non_measurable_reason"),
      col("view_detection_enabled")
        .cast(IntegerType)
        .as("view_detection_enabled"),
      col("device_make_id").cast(IntegerType).as("device_make_id"),
      col("operating_system_family_id")
        .cast(IntegerType)
        .as("operating_system_family_id"),
      col("pricing_media_type").cast(IntegerType).as("pricing_media_type"),
      col("buyer_trx_event_id").cast(IntegerType).as("buyer_trx_event_id"),
      col("seller_trx_event_id").cast(IntegerType).as("seller_trx_event_id"),
      col("is_unit_of_trx"),
      col("revenue_auction_event_type")
        .cast(IntegerType)
        .as("revenue_auction_event_type"),
      col("is_prebid"),
      col("auction_timestamp").cast(LongType).as("auction_timestamp"),
      col("two_phase_reduction_applied"),
      col("region_id").cast(IntegerType).as("region_id"),
      col("media_company_id").cast(IntegerType).as("media_company_id"),
      col("trade_agreement_id").cast(IntegerType).as("trade_agreement_id"),
      col("personal_data"),
      col("anonymized_user_info"),
      col("fx_rate_snapshot_id").cast(IntegerType).as("fx_rate_snapshot_id"),
      col("crossdevice_group_anon"),
      col("revenue_event_type_id")
        .cast(IntegerType)
        .as("revenue_event_type_id"),
      col("external_creative_id"),
      col("targeted_segment_details"),
      col("bidder_seat_id").cast(IntegerType).as("bidder_seat_id"),
      col("is_curated"),
      col("curator_member_id").cast(IntegerType).as("curator_member_id"),
      col("cold_start_price_type")
        .cast(IntegerType)
        .as("cold_start_price_type"),
      col("discovery_state").cast(IntegerType).as("discovery_state"),
      col("billing_period_id").cast(IntegerType).as("billing_period_id"),
      col("flight_id").cast(IntegerType).as("flight_id"),
      col("split_id").cast(IntegerType).as("split_id"),
      col("discovery_prediction"),
      col("campaign_group_type_id")
        .cast(IntegerType)
        .as("campaign_group_type_id"),
      col("excluded_targeted_segment_details"),
      col("predicted_kpi_event_rate"),
      col("has_crossdevice_reach_extension"),
      col("counterparty_ruleset_type")
        .cast(IntegerType)
        .as("counterparty_ruleset_type"),
      col("log_product_ads"),
      col("hb_source").cast(IntegerType).as("hb_source"),
      col("buyer_line_item_currency"),
      col("deal_line_item_currency"),
      col("personal_identifiers_experimental"),
      col("postal_code_ext_id").cast(IntegerType).as("postal_code_ext_id"),
      col("targeted_segment_details_by_id_type"),
      col("personal_identifiers"),
      col("buyer_dpvp_bitmap").cast(LongType).as("buyer_dpvp_bitmap"),
      col("seller_dpvp_bitmap").cast(LongType).as("seller_dpvp_bitmap")
    )

}
