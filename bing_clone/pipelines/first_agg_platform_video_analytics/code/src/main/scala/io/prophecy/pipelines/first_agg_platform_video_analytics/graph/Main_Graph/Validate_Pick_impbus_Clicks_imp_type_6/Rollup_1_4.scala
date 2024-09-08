package io.prophecy.pipelines.first_agg_platform_video_analytics.graph.Main_Graph.Validate_Pick_impbus_Clicks_imp_type_6

import io.prophecy.libs._
import io.prophecy.pipelines.first_agg_platform_video_analytics.udfs.PipelineInitCode._
import io.prophecy.pipelines.first_agg_platform_video_analytics.udfs.UDFs._
import io.prophecy.pipelines.first_agg_platform_video_analytics.udfs.ColumnFunctions._
import io.prophecy.pipelines.first_agg_platform_video_analytics.graph.Main_Graph.Validate_Pick_impbus_Clicks_imp_type_6.config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Rollup_1_4 {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.groupBy(col("auction_id_64").cast(LongType).as("auction_id_64"),
               col("imp_type").cast(IntegerType).as("imp_type")
      )
      .agg(
        max(col("date_time").cast(LongType)).as("date_time"),
        last(col("user_id_64")).cast(LongType).as("user_id_64"),
        last(col("tag_id")).cast(IntegerType).as("tag_id"),
        last(col("venue_id")).cast(IntegerType).as("venue_id"),
        last(col("inventory_source_id"))
          .cast(IntegerType)
          .as("inventory_source_id"),
        last(col("session_frequency"))
          .cast(IntegerType)
          .as("session_frequency"),
        last(col("width")).cast(IntegerType).as("width"),
        last(col("height")).cast(IntegerType).as("height"),
        last(col("geo_country")).as("geo_country"),
        last(col("geo_region")).as("geo_region"),
        last(col("gender")).as("gender"),
        last(col("age")).cast(IntegerType).as("age"),
        last(col("seller_member_id")).cast(IntegerType).as("seller_member_id"),
        last(col("buyer_member_id")).cast(IntegerType).as("buyer_member_id"),
        last(col("creative_id")).cast(IntegerType).as("creative_id"),
        last(col("seller_currency")).as("seller_currency"),
        last(col("buyer_currency")).as("buyer_currency"),
        last(col("advertiser_id")).cast(IntegerType).as("advertiser_id"),
        last(col("campaign_group_id"))
          .cast(IntegerType)
          .as("campaign_group_id"),
        last(col("campaign_id")).cast(IntegerType).as("campaign_id"),
        last(col("creative_freq")).cast(IntegerType).as("creative_freq"),
        last(col("creative_rec")).cast(IntegerType).as("creative_rec"),
        last(col("is_learn")).cast(IntegerType).as("is_learn"),
        last(col("is_remarketing")).cast(IntegerType).as("is_remarketing"),
        last(col("advertiser_frequency"))
          .cast(IntegerType)
          .as("advertiser_frequency"),
        last(col("advertiser_recency"))
          .cast(IntegerType)
          .as("advertiser_recency"),
        last(col("user_group_id")).cast(IntegerType).as("user_group_id"),
        last(col("camp_dp_id")).cast(IntegerType).as("camp_dp_id"),
        last(col("media_buy_id")).cast(IntegerType).as("media_buy_id"),
        last(col("brand_id")).cast(IntegerType).as("brand_id"),
        last(col("is_appnexus_cleared"))
          .cast(IntegerType)
          .as("is_appnexus_cleared"),
        last(col("clear_fees")).as("clear_fees"),
        last(col("media_buy_rev_share_pct")).as("media_buy_rev_share_pct"),
        last(col("revenue_value")).as("revenue_value"),
        last(col("pricing_type")).as("pricing_type"),
        last(col("site_id")).cast(IntegerType).as("site_id"),
        last(col("content_category_id"))
          .cast(IntegerType)
          .as("content_category_id"),
        last(col("fold_position")).cast(IntegerType).as("fold_position"),
        last(col("external_inv_id")).cast(IntegerType).as("external_inv_id"),
        last(col("cadence_modifier")).as("cadence_modifier"),
        last(col("advertiser_currency")).as("advertiser_currency"),
        last(col("advertiser_exchange_rate")).as("advertiser_exchange_rate"),
        last(col("ip_address")).as("ip_address"),
        last(col("pub_rule_id")).cast(IntegerType).as("pub_rule_id"),
        last(col("publisher_id")).cast(IntegerType).as("publisher_id"),
        last(col("insertion_order_id"))
          .cast(IntegerType)
          .as("insertion_order_id"),
        last(col("predict_type_rev")).cast(IntegerType).as("predict_type_rev"),
        last(col("predict_type_goal"))
          .cast(IntegerType)
          .as("predict_type_goal"),
        last(col("predict_type_cost"))
          .cast(IntegerType)
          .as("predict_type_cost"),
        last(col("commission_revshare")).as("commission_revshare"),
        last(col("serving_fees_revshare")).as("serving_fees_revshare"),
        last(col("user_tz_offset")).cast(IntegerType).as("user_tz_offset"),
        last(col("media_type")).cast(IntegerType).as("media_type"),
        last(col("operating_system")).cast(IntegerType).as("operating_system"),
        last(col("browser")).cast(IntegerType).as("browser"),
        last(col("language")).cast(IntegerType).as("language"),
        last(col("publisher_currency")).as("publisher_currency"),
        last(col("publisher_exchange_rate")).as("publisher_exchange_rate"),
        last(col("site_domain")).as("site_domain"),
        last(col("payment_type")).cast(IntegerType).as("payment_type"),
        last(col("revenue_type")).cast(IntegerType).as("revenue_type"),
        last(col("bidder_id")).cast(IntegerType).as("bidder_id"),
        last(col("inv_code")).as("inv_code"),
        last(col("application_id")).as("application_id"),
        last(col("is_control")).cast(IntegerType).as("is_control"),
        last(col("vp_expose_domains"))
          .cast(IntegerType)
          .as("vp_expose_domains"),
        last(col("vp_expose_categories"))
          .cast(IntegerType)
          .as("vp_expose_categories"),
        last(col("vp_expose_pubs")).cast(IntegerType).as("vp_expose_pubs"),
        last(col("vp_expose_tag")).cast(IntegerType).as("vp_expose_tag"),
        last(col("vp_expose_age")).cast(IntegerType).as("vp_expose_age"),
        last(col("vp_expose_gender")).cast(IntegerType).as("vp_expose_gender"),
        last(col("inventory_url_id")).cast(IntegerType).as("inventory_url_id"),
        last(col("imp_time")).as("imp_time"),
        last(col("is_exclusive")).cast(IntegerType).as("is_exclusive"),
        last(col("truncate_ip")).cast(IntegerType).as("truncate_ip"),
        last(col("datacenter_id")).cast(IntegerType).as("datacenter_id"),
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
        last(col("targeted_segments")).as("targeted_segments"),
        last(col("supply_type")).cast(IntegerType).as("supply_type"),
        last(col("is_toolbar")).cast(IntegerType).as("is_toolbar"),
        last(col("control_pct")).as("control_pct"),
        last(col("deal_id")).cast(IntegerType).as("deal_id"),
        last(col("vp_bitmap")).cast(LongType).as("vp_bitmap"),
        last(col("ozone_id")).cast(IntegerType).as("ozone_id"),
        last(col("is_performance")).cast(IntegerType).as("is_performance"),
        last(col("sdk_version")).as("sdk_version"),
        last(col("device_type")).cast(IntegerType).as("device_type"),
        last(col("dma")).cast(IntegerType).as("dma"),
        last(col("postal")).as("postal"),
        last(col("package_id")).cast(IntegerType).as("package_id"),
        last(col("campaign_group_freq"))
          .cast(IntegerType)
          .as("campaign_group_freq"),
        last(col("campaign_group_rec"))
          .cast(IntegerType)
          .as("campaign_group_rec"),
        last(col("insertion_order_freq"))
          .cast(IntegerType)
          .as("insertion_order_freq"),
        last(col("insertion_order_rec"))
          .cast(IntegerType)
          .as("insertion_order_rec"),
        last(col("buyer_gender")).as("buyer_gender"),
        last(col("buyer_age")).cast(IntegerType).as("buyer_age"),
        last(col("targeted_segment_list")).as("targeted_segment_list"),
        last(col("custom_model_id")).cast(IntegerType).as("custom_model_id"),
        last(col("custom_model_last_modified"))
          .cast(LongType)
          .as("custom_model_last_modified"),
        last(col("custom_model_output_code")).as("custom_model_output_code"),
        last(col("external_uid")).as("external_uid"),
        last(col("request_uuid")).as("request_uuid"),
        last(col("mobile_app_instance_id"))
          .cast(IntegerType)
          .as("mobile_app_instance_id"),
        last(col("traffic_source_code")).as("traffic_source_code"),
        last(col("external_request_id")).as("external_request_id"),
        last(col("stitch_group_id")).as("stitch_group_id"),
        last(col("deal_type")).cast(IntegerType).as("deal_type"),
        last(col("ym_floor_id")).cast(IntegerType).as("ym_floor_id"),
        last(col("ym_bias_id")).cast(IntegerType).as("ym_bias_id"),
        last(col("bid_priority")).cast(IntegerType).as("bid_priority"),
        last(col("viewdef_definition_id"))
          .cast(IntegerType)
          .as("viewdef_definition_id"),
        last(col("view_result")).cast(IntegerType).as("view_result"),
        last(col("view_non_measurable_reason"))
          .cast(IntegerType)
          .as("view_non_measurable_reason"),
        last(col("view_detection_enabled"))
          .cast(IntegerType)
          .as("view_detection_enabled"),
        last(col("device_make_id")).cast(IntegerType).as("device_make_id"),
        last(col("operating_system_family_id"))
          .cast(IntegerType)
          .as("operating_system_family_id"),
        last(col("pricing_media_type"))
          .cast(IntegerType)
          .as("pricing_media_type"),
        last(col("buyer_trx_event_id"))
          .cast(IntegerType)
          .as("buyer_trx_event_id"),
        last(col("seller_trx_event_id"))
          .cast(IntegerType)
          .as("seller_trx_event_id"),
        last(col("is_unit_of_trx")).cast(BooleanType).as("is_unit_of_trx"),
        last(col("revenue_auction_event_type"))
          .cast(IntegerType)
          .as("revenue_auction_event_type"),
        last(col("is_prebid")).cast(BooleanType).as("is_prebid"),
        last(col("auction_timestamp")).cast(LongType).as("auction_timestamp"),
        last(col("two_phase_reduction_applied"))
          .cast(BooleanType)
          .as("two_phase_reduction_applied"),
        last(col("region_id")).cast(IntegerType).as("region_id"),
        last(col("media_company_id")).cast(IntegerType).as("media_company_id"),
        last(col("trade_agreement_id"))
          .cast(IntegerType)
          .as("trade_agreement_id"),
        last(col("personal_data")).as("personal_data"),
        last(col("anonymized_user_info")).as("anonymized_user_info"),
        last(col("fx_rate_snapshot_id"))
          .cast(IntegerType)
          .as("fx_rate_snapshot_id"),
        last(col("crossdevice_group_anon")).as("crossdevice_group_anon"),
        last(col("revenue_event_type_id"))
          .cast(IntegerType)
          .as("revenue_event_type_id"),
        last(col("external_creative_id")).as("external_creative_id"),
        last(col("targeted_segment_details")).as("targeted_segment_details"),
        last(col("bidder_seat_id")).cast(IntegerType).as("bidder_seat_id"),
        last(col("is_curated")).cast(BooleanType).as("is_curated"),
        last(col("curator_member_id"))
          .cast(IntegerType)
          .as("curator_member_id"),
        last(col("cold_start_price_type"))
          .cast(IntegerType)
          .as("cold_start_price_type"),
        last(col("discovery_state")).cast(IntegerType).as("discovery_state"),
        last(col("billing_period_id"))
          .cast(IntegerType)
          .as("billing_period_id"),
        last(col("flight_id")).cast(IntegerType).as("flight_id"),
        last(col("split_id")).cast(IntegerType).as("split_id"),
        last(col("discovery_prediction")).as("discovery_prediction"),
        last(col("campaign_group_type_id"))
          .cast(IntegerType)
          .as("campaign_group_type_id"),
        last(col("excluded_targeted_segment_details"))
          .as("excluded_targeted_segment_details"),
        last(col("predicted_kpi_event_rate")).as("predicted_kpi_event_rate"),
        last(col("has_crossdevice_reach_extension"))
          .cast(BooleanType)
          .as("has_crossdevice_reach_extension"),
        last(col("counterparty_ruleset_type"))
          .cast(IntegerType)
          .as("counterparty_ruleset_type"),
        last(col("log_product_ads")).as("log_product_ads"),
        last(col("hb_source")).cast(IntegerType).as("hb_source"),
        last(col("buyer_line_item_currency")).as("buyer_line_item_currency"),
        last(col("deal_line_item_currency")).as("deal_line_item_currency"),
        last(col("personal_identifiers_experimental"))
          .as("personal_identifiers_experimental"),
        last(col("postal_code_ext_id"))
          .cast(IntegerType)
          .as("postal_code_ext_id"),
        last(col("targeted_segment_details_by_id_type"))
          .as("targeted_segment_details_by_id_type"),
        last(col("personal_identifiers")).as("personal_identifiers"),
        last(col("buyer_dpvp_bitmap")).cast(LongType).as("buyer_dpvp_bitmap"),
        last(col("seller_dpvp_bitmap")).cast(LongType).as("seller_dpvp_bitmap")
      )

}
