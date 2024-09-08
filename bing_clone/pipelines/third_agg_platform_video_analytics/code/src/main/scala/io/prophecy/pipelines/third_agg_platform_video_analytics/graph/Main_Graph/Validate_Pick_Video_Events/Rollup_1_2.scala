package io.prophecy.pipelines.third_agg_platform_video_analytics.graph.Main_Graph.Validate_Pick_Video_Events

import io.prophecy.libs._
import io.prophecy.pipelines.third_agg_platform_video_analytics.udfs.PipelineInitCode._
import io.prophecy.pipelines.third_agg_platform_video_analytics.udfs.UDFs._
import io.prophecy.pipelines.third_agg_platform_video_analytics.udfs.ColumnFunctions._
import io.prophecy.pipelines.third_agg_platform_video_analytics.graph.Main_Graph.Validate_Pick_Video_Events.config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Rollup_1_2 {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.groupBy(col("auction_id_64"), col("imp_type"), col("request_imp_type"))
      .agg(
        first(col("date_time")).as("date_time"),
        first(col("buyer_member_id")).as("buyer_member_id"),
        first(col("seller_member_id")).as("seller_member_id"),
        first(col("advertiser_id")).as("advertiser_id"),
        first(col("publisher_id")).as("publisher_id"),
        first(col("site_id")).as("site_id"),
        first(col("tag_id")).as("tag_id"),
        first(col("insertion_order_id")).as("insertion_order_id"),
        first(col("campaign_group_id")).as("campaign_group_id"),
        first(col("campaign_id")).as("campaign_id"),
        first(col("creative_id")).as("creative_id"),
        first(col("creative_freq")).as("creative_freq"),
        first(col("creative_rec")).as("creative_rec"),
        first(col("brand_id")).as("brand_id"),
        first(col("geo_country")).as("geo_country"),
        first(col("width")).as("width"),
        first(col("height")).as("height"),
        first(col("deal_id")).as("deal_id"),
        first(col("video_was_served")).as("video_was_served"),
        first(col("video_started")).as("video_started"),
        first(col("video_was_skipped")).as("video_was_skipped"),
        first(col("video_had_error")).as("video_had_error"),
        first(col("video_hit_25_pct")).as("video_hit_25_pct"),
        first(col("video_hit_50_pct")).as("video_hit_50_pct"),
        first(col("video_hit_75_pct")).as("video_hit_75_pct"),
        first(col("video_completed")).as("video_completed"),
        first(col("advertiser_currency")).as("advertiser_currency"),
        first(col("publisher_currency")).as("publisher_currency"),
        first(col("site_domain")).as("site_domain"),
        first(col("application_id")).as("application_id"),
        first(col("media_cost_dollars_cpm")).as("media_cost_dollars_cpm"),
        first(col("booked_revenue_dollars")).as("booked_revenue_dollars"),
        first(col("seller_revenue_cpm")).as("seller_revenue_cpm"),
        first(col("vp_bitmap")).as("vp_bitmap"),
        first(col("buyer_currency")).as("buyer_currency"),
        first(col("is_learn")).as("is_learn"),
        first(col("external_inv_id")).as("external_inv_id"),
        first(col("pub_rule_id")).as("pub_rule_id"),
        first(col("predict_type_rev")).as("predict_type_rev"),
        first(col("predict_type_goal")).as("predict_type_goal"),
        first(col("media_type")).as("media_type"),
        first(col("venue_id")).as("venue_id"),
        first(col("payment_type")).as("payment_type"),
        first(col("revenue_type")).as("revenue_type"),
        first(col("viewdef_definition_id")).as("viewdef_definition_id"),
        first(col("advertiser_exchange_rate")).as("advertiser_exchange_rate"),
        first(col("publisher_exchange_rate")).as("publisher_exchange_rate"),
        first(col("vp_expose_pubs")).as("vp_expose_pubs"),
        first(col("vp_expose_tag")).as("vp_expose_tag"),
        first(col("vp_expose_categories")).as("vp_expose_categories"),
        first(col("playback_method")).as("playback_method"),
        first(col("video_context")).as("video_context"),
        first(col("player_size_id")).as("player_size_id"),
        first(col("supply_type")).as("supply_type"),
        first(col("vp_expose_domains")).as("vp_expose_domains"),
        first(col("view_result")).as("view_result"),
        first(col("view_non_measurable_reason"))
          .as("view_non_measurable_reason"),
        first(col("error_code")).as("error_code"),
        first(col("call_type")).as("call_type"),
        first(col("companion_creative_id")).as("companion_creative_id"),
        first(col("companion_imps")).as("companion_imps"),
        first(col("companion_clicks")).as("companion_clicks"),
        first(col("user_id_64")).as("user_id_64"),
        first(col("geo_region")).as("geo_region"),
        first(col("gender")).as("gender"),
        first(col("age")).as("age"),
        first(col("operating_system")).as("operating_system"),
        first(col("browser")).as("browser"),
        first(col("language")).as("language"),
        first(col("latitude")).as("latitude"),
        first(col("longitude")).as("longitude"),
        first(col("device_id")).as("device_id"),
        first(col("carrier_id")).as("carrier_id"),
        first(col("device_type")).as("device_type"),
        first(col("dma")).as("dma"),
        first(col("postal")).as("postal"),
        first(col("city")).as("city"),
        first(col("request_uuid")).as("request_uuid"),
        first(col("tag_sizes")).as("tag_sizes"),
        first(col("user_tz_offset")).as("user_tz_offset"),
        first(col("fold_position")).as("fold_position"),
        first(col("anonymized_user_info")).as("anonymized_user_info"),
        first(col("personal_data")).as("personal_data"),
        first(col("inventory_url_id")).as("inventory_url_id"),
        first(col("predicted_100pd_video_completion_rate"))
          .as("predicted_100pd_video_completion_rate"),
        first(col("inventory_source_id")).as("inventory_source_id"),
        first(col("session_frequency")).as("session_frequency"),
        first(col("is_control")).as("is_control"),
        first(col("device_unique_id")).as("device_unique_id"),
        first(col("targeted_segments")).as("targeted_segments"),
        first(col("seller_currency")).as("seller_currency"),
        first(col("is_toolbar")).as("is_toolbar"),
        first(col("control_pct")).as("control_pct"),
        first(col("advertiser_frequency")).as("advertiser_frequency"),
        first(col("advertiser_recency")).as("advertiser_recency"),
        first(col("ozone_id")).as("ozone_id"),
        first(col("is_performance")).as("is_performance"),
        first(col("sdk_version")).as("sdk_version"),
        first(col("media_buy_id")).as("media_buy_id"),
        first(col("camp_dp_id")).as("camp_dp_id"),
        first(col("user_group_id")).as("user_group_id"),
        first(col("package_id")).as("package_id"),
        first(col("campaign_group_freq")).as("campaign_group_freq"),
        first(col("campaign_group_rec")).as("campaign_group_rec"),
        first(col("insertion_order_freq")).as("insertion_order_freq"),
        first(col("insertion_order_rec")).as("insertion_order_rec"),
        first(col("buyer_gender")).as("buyer_gender"),
        first(col("buyer_age")).as("buyer_age"),
        first(col("targeted_segment_list")).as("targeted_segment_list"),
        first(col("custom_model_id")).as("custom_model_id"),
        first(col("custom_model_last_modified"))
          .as("custom_model_last_modified"),
        first(col("custom_model_output_code")).as("custom_model_output_code"),
        first(col("external_uid")).as("external_uid"),
        first(col("is_remarketing")).as("is_remarketing"),
        first(col("mobile_app_instance_id")).as("mobile_app_instance_id"),
        first(col("traffic_source_code")).as("traffic_source_code"),
        first(col("external_request_id")).as("external_request_id"),
        first(col("stitch_group_id")).as("stitch_group_id"),
        first(col("deal_type")).as("deal_type"),
        first(col("ym_floor_id")).as("ym_floor_id"),
        first(col("ym_bias_id")).as("ym_bias_id"),
        first(col("bid_priority")).as("bid_priority"),
        first(col("pricing_type")).as("pricing_type"),
        first(col("buyer_charges")).as("buyer_charges"),
        first(col("seller_charges")).as("seller_charges"),
        first(col("revenue_value")).as("revenue_value"),
        first(col("media_buy_rev_share_pct")).as("media_buy_rev_share_pct"),
        first(col("view_detection_enabled")).as("view_detection_enabled"),
        first(col("data_costs")).as("data_costs"),
        first(col("device_make_id")).as("device_make_id"),
        first(col("operating_system_family_id"))
          .as("operating_system_family_id"),
        first(col("pricing_media_type")).as("pricing_media_type"),
        first(col("buyer_trx_event_id")).as("buyer_trx_event_id"),
        first(col("seller_trx_event_id")).as("seller_trx_event_id"),
        first(col("is_unit_of_trx")).as("is_unit_of_trx"),
        first(col("revenue_auction_event_type"))
          .as("revenue_auction_event_type"),
        first(col("is_prebid")).as("is_prebid"),
        first(col("auction_timestamp")).as("auction_timestamp"),
        first(col("clear_fees")).as("clear_fees"),
        first(col("is_appnexus_cleared")).as("is_appnexus_cleared"),
        first(col("two_phase_reduction_applied"))
          .as("two_phase_reduction_applied"),
        first(col("region_id")).as("region_id"),
        first(col("media_company_id")).as("media_company_id"),
        first(col("trade_agreement_id")).as("trade_agreement_id"),
        first(col("cadence_modifier")).as("cadence_modifier"),
        first(col("content_category_id")).as("content_category_id"),
        first(col("fx_rate_snapshot_id")).as("fx_rate_snapshot_id"),
        first(col("crossdevice_group_anon")).as("crossdevice_group_anon"),
        first(col("revenue_event_type_id")).as("revenue_event_type_id"),
        first(col("inv_code")).as("inv_code"),
        first(col("bidder_id")).as("bidder_id"),
        first(col("serving_fees_revshare")).as("serving_fees_revshare"),
        first(col("commission_revshare")).as("commission_revshare"),
        first(col("booked_revenue_adv_curr")).as("booked_revenue_adv_curr"),
        first(col("predict_type_cost")).as("predict_type_cost"),
        first(col("ip_address")).as("ip_address"),
        first(col("predict_goal")).as("predict_goal"),
        first(col("seller_deduction")).as("seller_deduction"),
        first(col("auction_service_fees")).as("auction_service_fees"),
        first(col("auction_service_deduction")).as("auction_service_deduction"),
        first(col("is_creative_hosted")).as("is_creative_hosted"),
        first(col("creative_audit_status")).as("creative_audit_status"),
        first(col("datacenter_id")).as("datacenter_id"),
        first(col("truncate_ip")).as("truncate_ip"),
        first(col("is_exclusive")).as("is_exclusive"),
        first(col("vp_expose_gender")).as("vp_expose_gender"),
        first(col("vp_expose_age")).as("vp_expose_age"),
        first(col("crossdevice_graph_cost")).as("crossdevice_graph_cost"),
        first(col("player_width")).as("player_width"),
        first(col("player_height")).as("player_height"),
        first(col("external_creative_id")).as("external_creative_id"),
        first(col("age_bucket")).as("age_bucket"),
        first(col("bidder_seat_id")).as("bidder_seat_id"),
        first(col("billing_period_id")).as("billing_period_id"),
        first(col("flight_id")).as("flight_id"),
        first(col("split_id")).as("split_id"),
        first(col("total_partner_fees_microcents"))
          .as("total_partner_fees_microcents"),
        first(col("net_media_cost_dollars_cpm"))
          .as("net_media_cost_dollars_cpm"),
        first(col("total_data_costs_microcents"))
          .as("total_data_costs_microcents"),
        first(col("total_profit_microcents")).as("total_profit_microcents"),
        first(col("curator_member_id")).as("curator_member_id"),
        first(col("eap")).as("eap"),
        first(col("ecp")).as("ecp"),
        first(col("buyer_bid")).as("buyer_bid"),
        first(col("reserve_price")).as("reserve_price"),
        first(col("serving_fees_cpm")).as("serving_fees_cpm"),
        first(col("can_convert")).as("can_convert"),
        first(col("control_creative_id")).as("control_creative_id"),
        first(col("commission_cpm")).as("commission_cpm"),
        first(col("creative_overage_fees")).as("creative_overage_fees"),
        first(col("imps_for_budget_caps_pacing"))
          .as("imps_for_budget_caps_pacing"),
        first(col("buyer_spend")).as("buyer_spend"),
        first(col("actual_bid")).as("actual_bid"),
        first(col("campaign_group_type_id")).as("campaign_group_type_id"),
        first(col("predicted_kpi_event_rate")).as("predicted_kpi_event_rate"),
        first(col("total_segment_data_costs_microcents"))
          .as("total_segment_data_costs_microcents"),
        first(col("total_feature_costs_microcents"))
          .as("total_feature_costs_microcents"),
        first(col("counterparty_ruleset_type")).as("counterparty_ruleset_type"),
        first(col("log_product_ads")).as("log_product_ads"),
        first(col("hb_source")).as("hb_source"),
        first(col("buyer_line_item_currency")).as("buyer_line_item_currency"),
        first(col("deal_line_item_currency")).as("deal_line_item_currency"),
        first(col("is_curated")).as("is_curated"),
        first(col("personal_identifiers_experimental"))
          .as("personal_identifiers_experimental"),
        first(col("postal_code_ext_id")).as("postal_code_ext_id"),
        first(col("ecpm_conversion_rate")).as("ecpm_conversion_rate"),
        first(col("targeted_segment_details_by_id_type"))
          .as("targeted_segment_details_by_id_type"),
        first(col("targeted_segment_details")).as("targeted_segment_details"),
        first(col("personal_identifiers")).as("personal_identifiers"),
        first(col("district_postal_code_lists"))
          .as("district_postal_code_lists"),
        first(col("video_served_timestamp")).as("video_served_timestamp"),
        first(col("video_started_timestamp")).as("video_started_timestamp"),
        first(col("video_skipped_timestamp")).as("video_skipped_timestamp"),
        first(col("video_errored_timestamp")).as("video_errored_timestamp"),
        first(col("video_hit_25_pct_timestamp"))
          .as("video_hit_25_pct_timestamp"),
        first(col("video_hit_50_pct_timestamp"))
          .as("video_hit_50_pct_timestamp"),
        first(col("video_hit_75_pct_timestamp"))
          .as("video_hit_75_pct_timestamp"),
        first(col("video_completed_timestamp")).as("video_completed_timestamp"),
        first(col("fallback_ad_index")).as("fallback_ad_index"),
        first(col("is_transacted")).as("is_transacted"),
        first(col("is_winning_events")).as("is_winning_events"),
        first(col("is_service_events")).as("is_service_events"),
        first(col("buyer_dpvp_bitmap")).as("buyer_dpvp_bitmap"),
        first(col("seller_dpvp_bitmap")).as("seller_dpvp_bitmap"),
        first(col("imp_media_cost_dollars_cpm"))
          .as("imp_media_cost_dollars_cpm"),
        first(col("imp_booked_revenue_dollars"))
          .as("imp_booked_revenue_dollars"),
        first(col("imp_seller_revenue_cpm")).as("imp_seller_revenue_cpm")
      )

}