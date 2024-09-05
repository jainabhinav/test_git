package com.microsoft.ads.data.dnv.agg_dw_clicks_pb.graph.External_Clicks

import io.prophecy.libs._
import com.microsoft.ads.data.dnv.agg_dw_clicks_pb.udfs.PipelineInitCode._
import com.microsoft.ads.data.dnv.agg_dw_clicks_pb.udfs.UDFs._
import com.microsoft.ads.data.dnv.agg_dw_clicks_pb.udfs.ColumnFunctions._
import com.microsoft.ads.data.dnv.agg_dw_clicks_pb.graph.External_Clicks.config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Join_aggImpbusClickPb {

  def apply(context: Context, left: DataFrame, right: DataFrame): DataFrame =
    left
      .as("left")
      .join(right.as("right"),
            col("left.auction_id_64") === col("right.auction_id_64"),
            "inner"
      )
      .select(
        when(col("left.date_time") =!= lit(0), col("left.date_time"))
          .as("date_time"),
        when(col("right.auction_id_64") =!= lit(0), col("right.auction_id_64"))
          .as("auction_id_64"),
        when(col("left.user_id_64") =!= lit(0), col("left.user_id_64"))
          .as("user_id_64"),
        when(col("right.tag_id") =!= lit(0),   col("right.tag_id")).as("tag_id"),
        when(col("right.venue_id") =!= lit(0), col("right.venue_id"))
          .as("venue_id"),
        when(col("right.inventory_source_id") =!= lit(0),
             col("right.inventory_source_id")
        ).as("inventory_source_id"),
        when(col("right.session_frequency") =!= lit(0),
             col("right.session_frequency")
        ).as("session_frequency"),
        when(col("right.width") =!= lit(0),  col("right.width")).as("width"),
        when(col("right.height") =!= lit(0), col("right.height")).as("height"),
        when(string_compare(col("right.geo_country"), lit("--")) === lit(1),
             col("right.geo_country")
        ).as("geo_country"),
        when(string_compare(col("right.geo_region"), lit("--")) === lit(1),
             col("right.geo_region")
        ).as("geo_region"),
        when(col("right.gender") =!= lit("u"), col("right.gender"))
          .as("gender"),
        when(col("right.age") =!= lit(0), col("right.age")).as("age"),
        when(col("right.seller_member_id") =!= lit(0),
             col("right.seller_member_id")
        ).as("seller_member_id"),
        when(col("right.buyer_member_id") =!= lit(0),
             col("right.buyer_member_id")
        ).as("buyer_member_id"),
        when(col("right.creative_id") =!= lit(0), col("right.creative_id"))
          .as("creative_id"),
        when(string_length(col("right.seller_currency")) > lit(0),
             col("right.seller_currency")
        ).as("seller_currency"),
        when(string_length(col("right.buyer_currency")) > lit(0),
             col("right.buyer_currency")
        ).as("buyer_currency"),
        when(col("right.advertiser_id") =!= lit(0), col("right.advertiser_id"))
          .as("advertiser_id"),
        when(col("right.campaign_group_id") =!= lit(0),
             col("right.campaign_group_id")
        ).as("campaign_group_id"),
        when(col("right.campaign_id") =!= lit(0), col("right.campaign_id"))
          .as("campaign_id"),
        when(col("right.creative_freq") =!= lit(0), col("right.creative_freq"))
          .as("creative_freq"),
        when(col("right.creative_rec") =!= lit(0), col("right.creative_rec"))
          .as("creative_rec"),
        when(col("right.is_learn") =!= lit(0), col("right.is_learn"))
          .as("is_learn"),
        when(col("right.is_remarketing") =!= lit(0),
             col("right.is_remarketing")
        ).as("is_remarketing"),
        when(col("right.advertiser_frequency") =!= lit(0),
             col("right.advertiser_frequency")
        ).as("advertiser_frequency"),
        when(col("right.advertiser_recency") =!= lit(0),
             col("right.advertiser_recency")
        ).as("advertiser_recency"),
        when(col("right.user_group_id") =!= lit(0), col("right.user_group_id"))
          .as("user_group_id"),
        when(col("right.camp_dp_id") =!= lit(0), col("right.camp_dp_id"))
          .as("camp_dp_id"),
        when(col("right.media_buy_id") =!= lit(0), col("right.media_buy_id"))
          .as("media_buy_id"),
        when(col("right.brand_id") =!= lit(0), col("right.brand_id"))
          .as("brand_id"),
        coalesce(col("right.cleared_direct"), lit(0))
          .cast(IntegerType)
          .as("is_appnexus_cleared"),
        col("right.clear_fees").as("clear_fees"),
        when(col("right.media_buy_rev_share_pct") =!= lit(0.0d),
             col("right.media_buy_rev_share_pct")
        ).as("media_buy_rev_share_pct"),
        when(col("right.revenue_value") =!= lit(0.0d),
             col("right.revenue_value")
        ).as("revenue_value"),
        coalesce(col("right.pricing_type"),   lit("--")).as("pricing_type"),
        when(col("right.site_id") =!= lit(0), col("right.site_id"))
          .as("site_id"),
        when(col("right.content_category_id") =!= lit(0),
             col("right.content_category_id")
        ).as("content_category_id"),
        when(col("right.fold_position") =!= lit(0), col("right.fold_position"))
          .as("fold_position"),
        when(col("right.external_inv_id") =!= lit(0),
             col("right.external_inv_id")
        ).as("external_inv_id"),
        when(col("right.cadence_modifier") =!= lit(0.0d),
             col("right.cadence_modifier")
        ).as("cadence_modifier"),
        col("right.imp_type").as("imp_type"),
        col("right.advertiser_currency").as("advertiser_currency"),
        col("right.advertiser_exchange_rate").as("advertiser_exchange_rate"),
        when(string_length(col("right.ip_address")) > lit(0),
             col("right.ip_address")
        ).as("ip_address"),
        when(col("right.pub_rule_id") =!= lit(0), col("right.pub_rule_id"))
          .as("pub_rule_id"),
        when(col("right.publisher_id") =!= lit(0), col("right.publisher_id"))
          .as("publisher_id"),
        when(col("right.insertion_order_id") =!= lit(0),
             col("right.insertion_order_id")
        ).as("insertion_order_id"),
        when(col("right.predict_type_rev") =!= lit(0),
             col("right.predict_type_rev")
        ).as("predict_type_rev"),
        when(col("right.predict_type_goal") =!= lit(0),
             col("right.predict_type_goal")
        ).as("predict_type_goal"),
        when(col("right.predict_type_cost") =!= lit(0),
             col("right.predict_type_cost")
        ).as("predict_type_cost"),
        when(col("right.commission_revshare") =!= lit(0.0d),
             col("right.commission_revshare")
        ).as("commission_revshare"),
        when(col("right.serving_fees_revshare") =!= lit(0.0d),
             col("right.serving_fees_revshare")
        ).as("serving_fees_revshare"),
        when(col("right.user_tz_offset") =!= lit(0),
             col("right.user_tz_offset")
        ).as("user_tz_offset"),
        when(col("right.media_type") =!= lit(0), col("right.media_type"))
          .as("media_type"),
        when(col("right.operating_system") =!= lit(0),
             col("right.operating_system")
        ).as("operating_system"),
        when(col("right.browser") =!= lit(0), col("right.browser"))
          .as("browser"),
        when(col("right.language") =!= lit(0), col("right.language"))
          .as("language"),
        when(string_length(col("right.publisher_currency")) > lit(0),
             col("right.publisher_currency")
        ).as("publisher_currency"),
        when(col("right.publisher_exchange_rate") =!= lit(0.0d),
             col("right.publisher_exchange_rate")
        ).as("publisher_exchange_rate"),
        when(col("right.site_domain") =!= lit("--"), col("right.site_domain"))
          .as("site_domain"),
        col("right.payment_type").as("payment_type"),
        col("right.revenue_type").as("revenue_type"),
        when(col("right.bidder_id") =!= lit(0), col("right.bidder_id"))
          .as("bidder_id"),
        when(col("right.inv_code") =!= lit("--"), col("right.inv_code"))
          .as("inv_code"),
        when(col("right.application_id") =!= lit("--"),
             col("right.application_id")
        ).as("application_id"),
        when(col("right.is_control") =!= lit(0), col("right.is_control"))
          .as("is_control"),
        when(col("right.vp_expose_domains") =!= lit(0),
             col("right.vp_expose_domains")
        ).as("vp_expose_domains"),
        when(col("right.vp_expose_categories") =!= lit(0),
             col("right.vp_expose_categories")
        ).as("vp_expose_categories"),
        when(col("right.vp_expose_pubs") =!= lit(0),
             col("right.vp_expose_pubs")
        ).as("vp_expose_pubs"),
        when(col("right.vp_expose_tag") =!= lit(0), col("right.vp_expose_tag"))
          .as("vp_expose_tag"),
        col("right.vp_expose_age").as("vp_expose_age"),
        col("right.vp_expose_gender").as("vp_expose_gender"),
        when(col("right.inventory_url_id") =!= lit(0),
             col("right.inventory_url_id")
        ).as("inventory_url_id"),
        from_unixtime(col("right.date_time"), "yyyy-MM-dd HH:mm:ss")
          .as("imp_time"),
        when(col("right.is_exclusive") =!= lit(0), col("right.is_exclusive"))
          .as("is_exclusive"),
        when(col("right.truncate_ip") =!= lit(0), col("right.truncate_ip"))
          .as("truncate_ip"),
        when(col("right.datacenter_id") =!= lit(0), col("right.datacenter_id"))
          .as("datacenter_id"),
        when(col("right.device_id") =!= lit(0), col("right.device_id"))
          .as("device_id"),
        when(col("right.carrier_id") =!= lit(0), col("right.carrier_id"))
          .as("carrier_id"),
        when(col("right.creative_audit_status") =!= lit(0),
             col("right.creative_audit_status")
        ).as("creative_audit_status"),
        when(col("right.is_creative_hosted") =!= lit(0),
             col("right.is_creative_hosted")
        ).as("is_creative_hosted"),
        when(col("right.city") =!= lit(0), col("right.city")).as("city"),
        when(string_length(col("right.latitude")) > lit(0),
             col("right.latitude")
        ).as("latitude"),
        when(string_length(col("right.longitude")) > lit(0),
             col("right.longitude")
        ).as("longitude"),
        when(string_length(col("right.device_unique_id")) > lit(0),
             col("right.device_unique_id")
        ).as("device_unique_id"),
        col("right.targeted_segments").as("targeted_segments"),
        when(col("right.supply_type") =!= lit(0), col("right.supply_type"))
          .as("supply_type"),
        when(col("right.is_toolbar") =!= lit(0), col("right.is_toolbar"))
          .as("is_toolbar"),
        when(col("right.control_pct") =!= lit(0.0d), col("right.control_pct"))
          .as("control_pct"),
        when(col("right.deal_id") =!= lit(0), col("right.deal_id"))
          .as("deal_id"),
        when(col("right.vp_bitmap") =!= lit(0), col("right.vp_bitmap"))
          .as("vp_bitmap"),
        when(col("right.ozone_id") =!= lit(0), col("right.ozone_id"))
          .as("ozone_id"),
        when(col("right.is_performance") =!= lit(0),
             col("right.is_performance")
        ).as("is_performance"),
        when(col("right.sdk_version") =!= lit("--"), col("right.sdk_version"))
          .as("sdk_version"),
        when(col("right.device_type") =!= lit(0), col("right.device_type"))
          .as("device_type"),
        when(col("right.dma") =!= lit(0),       col("right.dma")).as("dma"),
        when(col("right.postal") =!= lit("--"), col("right.postal"))
          .as("postal"),
        when(col("right.package_id") =!= lit(0), col("right.package_id"))
          .as("package_id"),
        when(col("right.campaign_group_freq") =!= lit(-2),
             col("right.campaign_group_freq")
        ).as("campaign_group_freq"),
        when(col("right.campaign_group_rec") =!= lit(0),
             col("right.campaign_group_rec")
        ).as("campaign_group_rec"),
        when(col("right.insertion_order_freq") =!= lit(-2),
             col("right.insertion_order_freq")
        ).as("insertion_order_freq"),
        when(col("right.insertion_order_rec") =!= lit(0),
             col("right.insertion_order_rec")
        ).as("insertion_order_rec"),
        when(col("right.buyer_gender") =!= lit("u"), col("right.buyer_gender"))
          .as("buyer_gender"),
        when(col("right.buyer_age") =!= lit(0), col("right.buyer_age"))
          .as("buyer_age"),
        when(size(col("right.targeted_segment_list")) > lit(0),
             col("right.targeted_segment_list")
        ).as("targeted_segment_list"),
        when(col("right.custom_model_id") =!= lit(0),
             col("right.custom_model_id")
        ).as("custom_model_id"),
        when(col("right.custom_model_last_modified") =!= lit(0),
             col("right.custom_model_last_modified")
        ).as("custom_model_last_modified"),
        when(string_length(col("right.custom_model_output_code")) > lit(0),
             col("right.custom_model_output_code")
        ).as("custom_model_output_code"),
        when(col("right.external_uid") =!= lit("--"), col("right.external_uid"))
          .as("external_uid"),
        when(col("right.request_uuid") =!= lit("--"), col("right.request_uuid"))
          .as("request_uuid"),
        when(col("right.mobile_app_instance_id") =!= lit(0),
             col("right.mobile_app_instance_id")
        ).as("mobile_app_instance_id"),
        when(col("right.traffic_source_code") =!= lit("--"),
             col("right.traffic_source_code")
        ).as("traffic_source_code"),
        col("right.external_request_id").as("external_request_id"),
        when(col("right.stitch_group_id") =!= lit("--"),
             col("right.stitch_group_id")
        ).as("stitch_group_id"),
        when(col("right.deal_type") =!= lit(0), col("right.deal_type"))
          .as("deal_type"),
        when(col("right.ym_floor_id") =!= lit(0), col("right.ym_floor_id"))
          .as("ym_floor_id"),
        when(col("right.ym_bias_id") =!= lit(0), col("right.ym_bias_id"))
          .as("ym_bias_id"),
        when(col("right.bid_priority") =!= lit(0), col("right.bid_priority"))
          .as("bid_priority"),
        when(col("right.viewdef_definition_id") =!= lit(0),
             col("right.viewdef_definition_id")
        ).as("viewdef_definition_id"),
        col("right.view_result").as("view_result"),
        col("right.view_non_measurable_reason")
          .as("view_non_measurable_reason"),
        when(col("right.view_detection_enabled") =!= lit(0),
             col("right.view_detection_enabled")
        ).as("view_detection_enabled"),
        when(col("right.device_make_id") =!= lit(0),
             col("right.device_make_id")
        ).as("device_make_id"),
        when(col("right.operating_system_family_id") =!= lit(1),
             col("right.operating_system_family_id")
        ).as("operating_system_family_id"),
        when(col("right.pricing_media_type") =!= lit(0),
             col("right.pricing_media_type")
        ).as("pricing_media_type"),
        when(col("right.buyer_trx_event_id") =!= lit(1),
             col("right.buyer_trx_event_id")
        ).as("buyer_trx_event_id"),
        when(col("right.seller_trx_event_id") =!= lit(1),
             col("right.seller_trx_event_id")
        ).as("seller_trx_event_id"),
        f_exchange_traded_is_console_event_unit_of_trx(
          lit(1),
          col("right.imp_type"),
          col("right.buyer_trx_event_id"),
          col("right.seller_trx_event_id")
        ).cast(BooleanType).as("is_unit_of_trx"),
        when(col("right.revenue_auction_event_type") =!= lit(0),
             col("right.revenue_auction_event_type")
        ).as("revenue_auction_event_type"),
        when(col("right.is_prebid").cast(ByteType) =!= lit(0),
             col("right.is_prebid").cast(BooleanType)
        ).as("is_prebid"),
        when(col("right.auction_timestamp") =!= lit(0),
             col("right.auction_timestamp")
        ).as("auction_timestamp"),
        when(col("right.two_phase_reduction_applied").cast(ByteType) =!= lit(0),
             col("right.two_phase_reduction_applied").cast(BooleanType)
        ).as("two_phase_reduction_applied"),
        when(col("right.region_id") =!= lit(0), col("right.region_id"))
          .as("region_id"),
        when(col("right.media_company_id") =!= lit(0),
             col("right.media_company_id")
        ).as("media_company_id"),
        when(col("right.trade_agreement_id") =!= lit(0),
             col("right.trade_agreement_id")
        ).as("trade_agreement_id"),
        col("right.personal_data").as("personal_data"),
        when(is_not_null(col("left.anonymized_user_info")),
             col("left.anonymized_user_info")
        ).as("anonymized_user_info"),
        when(col("right.fx_rate_snapshot_id") =!= lit(0),
             col("right.fx_rate_snapshot_id")
        ).as("fx_rate_snapshot_id"),
        when(is_not_null(col("right.crossdevice_group_anon")),
             col("right.crossdevice_group_anon")
        ).as("crossdevice_group_anon"),
        when(col("right.revenue_event_type_id") =!= lit(0),
             col("right.revenue_event_type_id")
        ).as("revenue_event_type_id"),
        col("right.external_creative_id").as("external_creative_id"),
        when(size(col("right.targeted_segment_details")) > lit(0),
             col("right.targeted_segment_details")
        ).as("targeted_segment_details"),
        when(col("right.bidder_seat_id") =!= lit(0),
             col("right.bidder_seat_id")
        ).as("bidder_seat_id"),
        when(col("right.is_curated").cast(ByteType) =!= lit(0),
             col("right.is_curated").cast(BooleanType)
        ).as("is_curated"),
        when(col("right.curator_member_id") =!= lit(0),
             col("right.curator_member_id")
        ).as("curator_member_id"),
        when(col("right.cold_start_price_type") =!= lit(-1),
             col("right.cold_start_price_type")
        ).as("cold_start_price_type"),
        when(col("right.discovery_state") =!= lit(-1),
             col("right.discovery_state")
        ).as("discovery_state"),
        when(col("right.billing_period_id") =!= lit(0),
             col("right.billing_period_id")
        ).as("billing_period_id"),
        when(col("right.flight_id") =!= lit(0), col("right.flight_id"))
          .as("flight_id"),
        when(col("right.split_id") =!= lit(0), col("right.split_id"))
          .as("split_id"),
        when(col("right.discovery_prediction") =!= lit(0.0d),
             col("right.discovery_prediction")
        ).as("discovery_prediction"),
        when(col("right.campaign_group_type_id") =!= lit(0),
             col("right.campaign_group_type_id")
        ).as("campaign_group_type_id"),
        col("right.excluded_targeted_segment_details")
          .as("excluded_targeted_segment_details"),
        col("right.predicted_kpi_event_rate").as("predicted_kpi_event_rate"),
        col("right.has_crossdevice_reach_extension")
          .cast(BooleanType)
          .as("has_crossdevice_reach_extension"),
        col("right.counterparty_ruleset_type").as("counterparty_ruleset_type"),
        col("right.log_product_ads").as("log_product_ads"),
        when(col("right.hb_source") =!= lit(0), col("right.hb_source"))
          .as("hb_source"),
        coalesce(col("right.buyer_line_item_currency"), lit("---"))
          .as("buyer_line_item_currency"),
        coalesce(col("right.deal_line_item_currency"), lit("---"))
          .as("deal_line_item_currency"),
        col("right.personal_identifiers_experimental")
          .as("personal_identifiers_experimental"),
        col("right.postal_code_ext_id").as("postal_code_ext_id"),
        col("right.targeted_segment_details_by_id_type")
          .as("targeted_segment_details_by_id_type"),
        col("right.personal_identifiers").as("personal_identifiers"),
        lit(null).cast(LongType).as("buyer_dpvp_bitmap"),
        lit(null).cast(LongType).as("seller_dpvp_bitmap")
      )

}
