package io.prophecy.pipelines.second_agg_platform_video_analytics.graph.Main_Graph

import io.prophecy.libs._
import io.prophecy.pipelines.second_agg_platform_video_analytics.udfs.PipelineInitCode._
import io.prophecy.pipelines.second_agg_platform_video_analytics.udfs.UDFs._
import io.prophecy.pipelines.second_agg_platform_video_analytics.udfs.ColumnFunctions._
import io.prophecy.pipelines.second_agg_platform_video_analytics.graph.Main_Graph.config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Reformat_5 {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.select(
      col("date_time"),
      col("auction_id_64"),
      col("user_id_64"),
      col("tag_id"),
      col("venue_id"),
      col("inventory_source_id"),
      col("session_frequency"),
      col("width"),
      col("height"),
      col("geo_country"),
      col("geo_region"),
      col("gender"),
      col("age"),
      col("seller_member_id"),
      col("buyer_member_id"),
      col("creative_id"),
      col("seller_currency"),
      col("buyer_currency"),
      col("advertiser_id"),
      col("campaign_group_id"),
      col("campaign_id"),
      col("creative_freq"),
      col("creative_rec"),
      col("is_learn"),
      col("is_remarketing"),
      col("advertiser_frequency"),
      col("advertiser_recency"),
      col("user_group_id"),
      col("camp_dp_id"),
      col("media_buy_id"),
      col("post_click_conv"),
      col("post_view_conv"),
      col("post_click_revenue"),
      col("post_view_revenue"),
      col("brand_id"),
      col("is_appnexus_cleared"),
      col("clear_fees"),
      col("media_buy_rev_share_pct"),
      col("revenue_value"),
      col("pricing_type"),
      col("imp_time"),
      col("pixel_id"),
      col("booked_revenue"),
      col("site_id"),
      col("content_category_id"),
      col("fold_position"),
      col("external_inv_id"),
      col("cadence_modifier"),
      col("predict_goal"),
      col("imp_type"),
      col("advertiser_currency"),
      col("advertiser_exchange_rate"),
      col("ip_address"),
      col("order_id"),
      col("external_data"),
      col("pub_rule_id"),
      col("publisher_id"),
      col("insertion_order_id"),
      col("predict_type_rev"),
      col("predict_type_goal"),
      col("predict_type_cost"),
      col("booked_revenue_dollars"),
      col("booked_revenue_adv_curr"),
      col("commission_revshare"),
      col("serving_fees_revshare"),
      col("is_control"),
      col("user_tz_offset"),
      col("media_type"),
      col("operating_system"),
      col("browser"),
      col("language"),
      col("publisher_currency"),
      col("publisher_exchange_rate"),
      col("media_cost_dollars_cpm"),
      col("site_domain"),
      col("payment_type"),
      col("revenue_type"),
      col("datacenter_id"),
      col("vp_expose_domains"),
      col("vp_expose_categories"),
      col("vp_expose_pubs"),
      col("vp_expose_tag"),
      col("vp_expose_age"),
      col("vp_expose_gender"),
      col("inventory_url_id"),
      col("is_exclusive"),
      col("truncate_ip"),
      col("device_id"),
      col("carrier_id"),
      col("creative_audit_status"),
      col("is_creative_hosted"),
      col("auction_service_deduction"),
      col("auction_service_fees"),
      col("seller_deduction"),
      col("city"),
      col("latitude"),
      col("longitude"),
      col("device_unique_id"),
      col("targeted_segments"),
      col("supply_type"),
      col("is_toolbar"),
      col("deal_id"),
      when(col("vp_bitmap") === lit(0), lit(null).cast(LongType))
        .otherwise(col("vp_bitmap"))
        .as("vp_bitmap"),
      col("application_id"),
      col("ozone_id"),
      col("is_performance"),
      col("sdk_version"),
      col("device_type"),
      col("dma"),
      col("postal"),
      col("package_id"),
      col("campaign_group_freq"),
      col("campaign_group_rec"),
      col("insertion_order_freq"),
      col("insertion_order_rec"),
      col("buyer_gender"),
      col("buyer_age"),
      col("targeted_segment_list"),
      col("custom_model_id"),
      col("custom_model_last_modified"),
      col("custom_model_output_code"),
      col("external_uid"),
      col("request_uuid"),
      when(col("mobile_app_instance_id") === lit(0),
           lit(null).cast(IntegerType)
      ).otherwise(col("mobile_app_instance_id")).as("mobile_app_instance_id"),
      col("traffic_source_code"),
      col("external_request_id"),
      col("stitch_group_id"),
      col("deal_type"),
      col("ym_floor_id"),
      col("ym_bias_id"),
      col("bid_priority"),
      col("viewdef_definition_id"),
      col("buyer_charges"),
      col("seller_charges"),
      col("view_result"),
      col("view_non_measurable_reason"),
      col("view_detection_enabled"),
      col("data_costs"),
      col("device_make_id"),
      col("operating_system_family_id"),
      col("bidder_id"),
      col("pricing_media_type"),
      col("buyer_trx_event_id"),
      col("seller_trx_event_id"),
      col("is_unit_of_trx"),
      col("revenue_auction_event_type"),
      col("is_prebid"),
      col("attribution_context"),
      col("two_phase_reduction_applied"),
      col("region_id"),
      col("media_company_id"),
      col("trade_agreement_id"),
      col("personal_data"),
      col("anonymized_user_info"),
      col("auction_timestamp"),
      col("fx_rate_snapshot_id"),
      col("crossdevice_group_anon"),
      col("post_click_crossdevice_conv"),
      col("post_view_crossdevice_conv"),
      col("revenue_event_type_id"),
      col("post_click_crossdevice_revenue"),
      col("post_view_crossdevice_revenue"),
      col("external_creative_id"),
      col("targeted_segment_details"),
      col("bidder_seat_id"),
      col("universal_pixel_rule_version_id"),
      col("is_curated"),
      col("curator_member_id"),
      col("cold_start_price_type"),
      col("discovery_state"),
      col("billing_period_id"),
      col("flight_id"),
      col("split_id"),
      col("conversion_device_type"),
      col("conversion_device_make_id"),
      col("universal_pixel_fire_id"),
      col("discovery_prediction"),
      col("campaign_group_type_id"),
      col("excluded_targeted_segment_details"),
      col("trust_id"),
      col("predicted_kpi_event_rate"),
      col("has_crossdevice_reach_extension"),
      col("counterparty_ruleset_type"),
      col("log_product_ads"),
      col("hb_source"),
      col("personal_identifiers_experimental"),
      col("postal_code_ext_id"),
      col("targeted_segment_details_by_id_type"),
      col("personal_identifiers"),
      col("post_click_ip_conv"),
      col("post_view_ip_conv"),
      col("hashed_ip"),
      col("district_postal_code_lists"),
      col("buyer_dpvp_bitmap"),
      col("seller_dpvp_bitmap"),
      col("private_auction_eligible"),
      col("chrome_traffic_label"),
      col("is_private_auction")
    )

}
