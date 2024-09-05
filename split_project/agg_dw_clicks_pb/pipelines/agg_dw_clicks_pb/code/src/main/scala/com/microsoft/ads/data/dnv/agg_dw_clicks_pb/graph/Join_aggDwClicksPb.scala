package com.microsoft.ads.data.dnv.agg_dw_clicks_pb.graph

import io.prophecy.libs._
import com.microsoft.ads.data.dnv.agg_dw_clicks_pb.udfs.PipelineInitCode._
import com.microsoft.ads.data.dnv.agg_dw_clicks_pb.udfs.UDFs._
import com.microsoft.ads.data.dnv.agg_dw_clicks_pb.udfs.ColumnFunctions._
import com.microsoft.ads.data.dnv.agg_dw_clicks_pb.config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Join_aggDwClicksPb {

  def apply(
    context: Context,
    in0:     DataFrame,
    in1:     DataFrame,
    in2:     DataFrame
  ): DataFrame =
    in0
      .as("in0")
      .join(in1.as("in1"),
            col("in0.auction_id_64") === col("in1.auction_id_64"),
            "inner"
      )
      .join(in2.as("in2"),
            col("in1.auction_id_64") === col("in2.auction_id_64"),
            "left_outer"
      )
      .select(
        when(col("in0.date_time") =!= lit(0), col("in0.date_time"))
          .as("date_time"),
        when(col("in1.auction_id_64") =!= lit(0), col("in1.auction_id_64"))
          .as("auction_id_64"),
        when(col("in0.user_id_64") =!= lit(0), col("in0.user_id_64"))
          .as("user_id_64"),
        when(col("in1.tag_id") =!= lit(0),   col("in1.tag_id")).as("tag_id"),
        when(col("in1.venue_id") =!= lit(0), col("in1.venue_id"))
          .as("venue_id"),
        when(col("in1.inventory_source_id") =!= lit(0),
             col("in1.inventory_source_id")
        ).as("inventory_source_id"),
        when(col("in1.session_frequency") =!= lit(0),
             col("in1.session_frequency")
        ).as("session_frequency"),
        when(col("in1.width") =!= lit(0),  col("in1.width")).as("width"),
        when(col("in1.height") =!= lit(0), col("in1.height")).as("height"),
        when(string_compare(col("in1.geo_country"), lit("--")) === lit(1),
             col("in1.geo_country")
        ).as("geo_country"),
        when(string_compare(col("in1.geo_region"), lit("--")) === lit(1),
             col("in1.geo_region")
        ).as("geo_region"),
        when(col("in1.gender") =!= lit("u"), col("in1.gender")).as("gender"),
        when(col("in1.age") =!= lit(0),      col("in1.age")).as("age"),
        when(col("in1.seller_member_id") =!= lit(0),
             col("in1.seller_member_id")
        ).as("seller_member_id"),
        when(col("in1.buyer_member_id") =!= lit(0), col("in1.buyer_member_id"))
          .as("buyer_member_id"),
        when(col("in1.creative_id") =!= lit(0), col("in1.creative_id"))
          .as("creative_id"),
        when(string_length(col("in1.seller_currency")) > lit(0),
             col("in1.seller_currency")
        ).as("seller_currency"),
        when(string_length(col("in1.buyer_currency")) > lit(0),
             col("in1.buyer_currency")
        ).as("buyer_currency"),
        when(col("in1.advertiser_id") =!= lit(0), col("in1.advertiser_id"))
          .as("advertiser_id"),
        when(col("in1.campaign_group_id") =!= lit(0),
             col("in1.campaign_group_id")
        ).as("campaign_group_id"),
        when(col("in1.campaign_id") =!= lit(0), col("in1.campaign_id"))
          .as("campaign_id"),
        when(col("in1.creative_freq") =!= lit(0), col("in1.creative_freq"))
          .as("creative_freq"),
        when(col("in1.creative_rec") =!= lit(0), col("in1.creative_rec"))
          .as("creative_rec"),
        when(col("in1.is_learn") =!= lit(0), col("in1.is_learn"))
          .as("is_learn"),
        when(col("in1.is_remarketing") =!= lit(0), col("in1.is_remarketing"))
          .as("is_remarketing"),
        when(col("in1.advertiser_frequency") =!= lit(0),
             col("in1.advertiser_frequency")
        ).as("advertiser_frequency"),
        when(col("in1.advertiser_recency") =!= lit(0),
             col("in1.advertiser_recency")
        ).as("advertiser_recency"),
        when(col("in1.user_group_id") =!= lit(0), col("in1.user_group_id"))
          .as("user_group_id"),
        when(col("in1.camp_dp_id") =!= lit(0), col("in1.camp_dp_id"))
          .as("camp_dp_id"),
        when(col("in1.media_buy_id") =!= lit(0), col("in1.media_buy_id"))
          .as("media_buy_id"),
        when(col("in1.brand_id") =!= lit(0), col("in1.brand_id"))
          .as("brand_id"),
        coalesce(col("in1.cleared_direct"), lit(0)).as("is_appnexus_cleared"),
        col("in1.clear_fees").as("clear_fees"),
        when(col("in1.media_buy_rev_share_pct") =!= lit(0.0d),
             col("in1.media_buy_rev_share_pct")
        ).as("media_buy_rev_share_pct"),
        when(col("in1.revenue_value") =!= lit(0.0d), col("in1.revenue_value"))
          .as("revenue_value"),
        coalesce(col("in1.pricing_type"),   lit("--")).as("pricing_type"),
        when(col("in1.site_id") =!= lit(0), col("in1.site_id")).as("site_id"),
        when(col("in1.content_category_id") =!= lit(0),
             col("in1.content_category_id")
        ).as("content_category_id"),
        when(col("in1.fold_position") =!= lit(0), col("in1.fold_position"))
          .as("fold_position"),
        when(col("in1.external_inv_id") =!= lit(0), col("in1.external_inv_id"))
          .as("external_inv_id"),
        when(col("in1.cadence_modifier") =!= lit(0.0d),
             col("in1.cadence_modifier")
        ).as("cadence_modifier"),
        lit(null).cast(StringType).as("predict_type"),
        lit(null).cast(DoubleType).as("predict_goal"),
        col("in1.imp_type").as("imp_type"),
        col("in1.advertiser_currency").as("advertiser_currency"),
        col("in1.advertiser_exchange_rate").as("advertiser_exchange_rate"),
        when(string_length(col("in1.ip_address")) > lit(0),
             col("in1.ip_address")
        ).as("ip_address"),
        when(col("in1.pub_rule_id") =!= lit(0), col("in1.pub_rule_id"))
          .as("pub_rule_id"),
        when(col("in1.publisher_id") =!= lit(0), col("in1.publisher_id"))
          .as("publisher_id"),
        when(col("in1.insertion_order_id") =!= lit(0),
             col("in1.insertion_order_id")
        ).as("insertion_order_id"),
        when(col("in1.predict_type_rev") =!= lit(0),
             col("in1.predict_type_rev")
        ).as("predict_type_rev"),
        when(col("in1.predict_type_goal") =!= lit(0),
             col("in1.predict_type_goal")
        ).as("predict_type_goal"),
        when(col("in1.predict_type_cost") =!= lit(0),
             col("in1.predict_type_cost")
        ).as("predict_type_cost"),
        lit(null).as("booked_revenue_dollars"),
        lit(null).as("booked_revenue_adv_curr"),
        when(col("in1.commission_revshare") =!= lit(0.0d),
             col("in1.commission_revshare")
        ).as("commission_revshare"),
        when(col("in1.serving_fees_revshare") =!= lit(0.0d),
             col("in1.serving_fees_revshare")
        ).as("serving_fees_revshare"),
        when(col("in1.user_tz_offset") =!= lit(0), col("in1.user_tz_offset"))
          .as("user_tz_offset"),
        when(col("in1.media_type") =!= lit(0), col("in1.media_type"))
          .as("media_type"),
        when(col("in1.operating_system") =!= lit(0),
             col("in1.operating_system")
        ).as("operating_system"),
        when(col("in1.browser") =!= lit(0),  col("in1.browser")).as("browser"),
        when(col("in1.language") =!= lit(0), col("in1.language"))
          .as("language"),
        when(string_length(col("in1.publisher_currency")) > lit(0),
             col("in1.publisher_currency")
        ).as("publisher_currency"),
        when(col("in1.publisher_exchange_rate") =!= lit(0.0d),
             col("in1.publisher_exchange_rate")
        ).as("publisher_exchange_rate"),
        lit(null).as("media_cost_dollars_cpm"),
        when(col("in1.site_domain") =!= lit("--"), col("in1.site_domain"))
          .as("site_domain"),
        col("in1.payment_type").as("payment_type"),
        col("in1.revenue_type").as("revenue_type"),
        when(col("in1.bidder_id") =!= lit(0), col("in1.bidder_id"))
          .as("bidder_id"),
        when(col("in1.inv_code") =!= lit("--"), col("in1.inv_code"))
          .as("inv_code"),
        when(col("in1.application_id") =!= lit("--"), col("in1.application_id"))
          .as("application_id"),
        when(col("in1.is_control") =!= lit(0), col("in1.is_control"))
          .as("is_control"),
        when(col("in1.vp_expose_domains") =!= lit(0),
             col("in1.vp_expose_domains")
        ).as("vp_expose_domains"),
        when(col("in1.vp_expose_categories") =!= lit(0),
             col("in1.vp_expose_categories")
        ).as("vp_expose_categories"),
        when(col("in1.vp_expose_pubs") =!= lit(0), col("in1.vp_expose_pubs"))
          .as("vp_expose_pubs"),
        when(col("in1.vp_expose_tag") =!= lit(0), col("in1.vp_expose_tag"))
          .as("vp_expose_tag"),
        col("in1.vp_expose_age").as("vp_expose_age"),
        col("in1.vp_expose_gender").as("vp_expose_gender"),
        when(col("in1.inventory_url_id") =!= lit(0),
             col("in1.inventory_url_id")
        ).as("inventory_url_id"),
        from_unixtime(col("in1.date_time"), "yyyy-MM-dd HH:mm:ss")
          .as("imp_time"),
        when(col("in1.is_exclusive") =!= lit(0), col("in1.is_exclusive"))
          .as("is_exclusive"),
        when(col("in1.truncate_ip") =!= lit(0), col("in1.truncate_ip"))
          .as("truncate_ip"),
        when(col("in1.datacenter_id") =!= lit(0), col("in1.datacenter_id"))
          .as("datacenter_id"),
        when(col("in1.device_id") =!= lit(0), col("in1.device_id"))
          .as("device_id"),
        when(col("in1.carrier_id") =!= lit(0), col("in1.carrier_id"))
          .as("carrier_id"),
        when(col("in1.creative_audit_status") =!= lit(0),
             col("in1.creative_audit_status")
        ).as("creative_audit_status"),
        when(col("in1.is_creative_hosted") =!= lit(0),
             col("in1.is_creative_hosted")
        ).as("is_creative_hosted"),
        lit(null).as("auction_service_deduction"),
        lit(null).as("auction_service_fees"),
        lit(null).as("seller_deduction"),
        when(col("in1.city") =!= lit(0),                  col("in1.city")).as("city"),
        when(string_length(col("in1.latitude")) > lit(0), col("in1.latitude"))
          .as("latitude"),
        when(string_length(col("in1.longitude")) > lit(0), col("in1.longitude"))
          .as("longitude"),
        when(string_length(col("in1.device_unique_id")) > lit(0),
             col("in1.device_unique_id")
        ).as("device_unique_id"),
        col("in1.targeted_segments").as("targeted_segments"),
        when(col("in1.supply_type") =!= lit(0), col("in1.supply_type"))
          .as("supply_type"),
        when(col("in1.is_toolbar") =!= lit(0), col("in1.is_toolbar"))
          .as("is_toolbar"),
        when(col("in1.control_pct") =!= lit(0.0d), col("in1.control_pct"))
          .as("control_pct"),
        when(col("in1.deal_id") =!= lit(0),   col("in1.deal_id")).as("deal_id"),
        when(col("in1.vp_bitmap") =!= lit(0), col("in1.vp_bitmap"))
          .as("vp_bitmap"),
        when(col("in1.ozone_id") =!= lit(0), col("in1.ozone_id"))
          .as("ozone_id"),
        when(col("in1.is_performance") =!= lit(0), col("in1.is_performance"))
          .as("is_performance"),
        when(col("in1.sdk_version") =!= lit("--"), col("in1.sdk_version"))
          .as("sdk_version"),
        when(col("in1.device_type") =!= lit(0), col("in1.device_type"))
          .as("device_type"),
        when(col("in1.dma") =!= lit(0),        col("in1.dma")).as("dma"),
        when(col("in1.postal") =!= lit("--"),  col("in1.postal")).as("postal"),
        when(col("in1.package_id") =!= lit(0), col("in1.package_id"))
          .as("package_id"),
        when(col("in1.campaign_group_freq") =!= lit(-2),
             col("in1.campaign_group_freq")
        ).as("campaign_group_freq"),
        when(col("in1.campaign_group_rec") =!= lit(0),
             col("in1.campaign_group_rec")
        ).as("campaign_group_rec"),
        when(col("in1.insertion_order_freq") =!= lit(-2),
             col("in1.insertion_order_freq")
        ).as("insertion_order_freq"),
        when(col("in1.insertion_order_rec") =!= lit(0),
             col("in1.insertion_order_rec")
        ).as("insertion_order_rec"),
        when(col("in1.buyer_gender") =!= lit("u"), col("in1.buyer_gender"))
          .as("buyer_gender"),
        when(col("in1.buyer_age") =!= lit(0), col("in1.buyer_age"))
          .as("buyer_age"),
        when(size(col("in1.targeted_segment_list")) > lit(0),
             col("in1.targeted_segment_list")
        ).as("targeted_segment_list"),
        when(col("in1.custom_model_id") =!= lit(0), col("in1.custom_model_id"))
          .as("custom_model_id"),
        when(col("in1.custom_model_last_modified") =!= lit(0),
             col("in1.custom_model_last_modified")
        ).as("custom_model_last_modified"),
        when(string_length(col("in1.custom_model_output_code")) > lit(0),
             col("in1.custom_model_output_code")
        ).as("custom_model_output_code"),
        when(col("in1.external_uid") =!= lit("--"), col("in1.external_uid"))
          .as("external_uid"),
        when(col("in1.request_uuid") =!= lit("--"), col("in1.request_uuid"))
          .as("request_uuid"),
        when(col("in1.mobile_app_instance_id") =!= lit(0),
             col("in1.mobile_app_instance_id")
        ).as("mobile_app_instance_id"),
        when(col("in1.traffic_source_code") =!= lit("--"),
             col("in1.traffic_source_code")
        ).as("traffic_source_code"),
        lit(null).cast(StringType).as("external_request_id"),
        when(col("in1.stitch_group_id") =!= lit("--"),
             col("in1.stitch_group_id")
        ).as("stitch_group_id"),
        when(col("in1.deal_type") =!= lit(0), col("in1.deal_type"))
          .as("deal_type"),
        when(col("in1.ym_floor_id") =!= lit(0), col("in1.ym_floor_id"))
          .as("ym_floor_id"),
        when(col("in1.ym_bias_id") =!= lit(0), col("in1.ym_bias_id"))
          .as("ym_bias_id"),
        when(col("in1.bid_priority") =!= lit(0), col("in1.bid_priority"))
          .as("bid_priority"),
        when(col("in1.viewdef_definition_id") =!= lit(0),
             col("in1.viewdef_definition_id")
        ).as("viewdef_definition_id"),
        lit(null).as("buyer_charges"),
        lit(null).as("seller_charges"),
        col("in1.view_result").as("view_result"),
        col("in1.view_non_measurable_reason").as("view_non_measurable_reason"),
        when(col("in1.view_detection_enabled") =!= lit(0),
             col("in1.view_detection_enabled")
        ).as("view_detection_enabled"),
        lit(null).as("data_costs"),
        when(col("in1.device_make_id") =!= lit(0), col("in1.device_make_id"))
          .as("device_make_id"),
        when(col("in1.operating_system_family_id") =!= lit(1),
             col("in1.operating_system_family_id")
        ).as("operating_system_family_id"),
        when(col("in1.pricing_media_type") =!= lit(0),
             col("in1.pricing_media_type")
        ).as("pricing_media_type"),
        when(col("in1.buyer_trx_event_id") =!= lit(1),
             col("in1.buyer_trx_event_id")
        ).as("buyer_trx_event_id"),
        when(col("in1.seller_trx_event_id") =!= lit(1),
             col("in1.seller_trx_event_id")
        ).as("seller_trx_event_id"),
        f_exchange_traded_is_console_event_unit_of_trx(
          lit(1),
          col("in1.imp_type"),
          col("in1.buyer_trx_event_id"),
          col("in1.seller_trx_event_id")
        ).cast(BooleanType).as("is_unit_of_trx"),
        when(col("in1.revenue_auction_event_type") =!= lit(0),
             col("in1.revenue_auction_event_type")
        ).as("revenue_auction_event_type"),
        when(col("in1.is_prebid") =!= lit(0),
             col("in1.is_prebid").cast(BooleanType)
        ).as("is_prebid"),
        when(col("in1.auction_timestamp") =!= lit(0),
             col("in1.auction_timestamp")
        ).as("auction_timestamp"),
        when(col("in1.two_phase_reduction_applied") =!= lit(0),
             col("in1.two_phase_reduction_applied").cast(BooleanType)
        ).as("two_phase_reduction_applied"),
        when(col("in1.region_id") =!= lit(0), col("in1.region_id"))
          .as("region_id"),
        when(col("in1.media_company_id") =!= lit(0),
             col("in1.media_company_id")
        ).as("media_company_id"),
        when(col("in1.trade_agreement_id") =!= lit(0),
             col("in1.trade_agreement_id")
        ).as("trade_agreement_id"),
        col("in1.personal_data").as("personal_data"),
        when(is_not_null(col("in0.anonymized_user_info")),
             col("in0.anonymized_user_info")
        ).as("anonymized_user_info"),
        when(col("in1.fx_rate_snapshot_id") =!= lit(0),
             col("in1.fx_rate_snapshot_id")
        ).as("fx_rate_snapshot_id"),
        when(is_not_null(col("in1.crossdevice_group_anon")),
             col("in1.crossdevice_group_anon")
        ).as("crossdevice_group_anon"),
        when(col("in1.revenue_event_type_id") =!= lit(0),
             col("in1.revenue_event_type_id")
        ).as("revenue_event_type_id"),
        lit(null).cast(StringType).as("external_creative_id"),
        when(size(col("in1.targeted_segment_details")) > lit(0),
             col("in1.targeted_segment_details")
        ).as("targeted_segment_details"),
        when(col("in1.bidder_seat_id") =!= lit(0), col("in1.bidder_seat_id"))
          .as("bidder_seat_id"),
        when(col("in1.is_curated") =!= lit(0),
             col("in1.is_curated").cast(BooleanType)
        ).as("is_curated"),
        when(col("in1.curator_member_id") =!= lit(0),
             col("in1.curator_member_id")
        ).as("curator_member_id"),
        when(col("in1.cold_start_price_type") =!= lit(-1),
             col("in1.cold_start_price_type")
        ).as("cold_start_price_type"),
        when(col("in1.discovery_state") =!= lit(-1), col("in1.discovery_state"))
          .as("discovery_state"),
        when(col("in1.billing_period_id") =!= lit(0),
             col("in1.billing_period_id")
        ).as("billing_period_id"),
        when(col("in1.flight_id") =!= lit(0), col("in1.flight_id"))
          .as("flight_id"),
        when(col("in1.split_id") =!= lit(0), col("in1.split_id"))
          .as("split_id"),
        lit(null).as("total_partner_fees_microcents"),
        col("in1.net_buyer_spend").as("net_buyer_spend"),
        lit(null).as("net_media_cost_dollars_cpm"),
        lit(null).as("total_data_costs_microcents"),
        lit(null).as("total_profit_microcents"),
        when(col("in1.discovery_prediction") =!= lit(0.0d),
             col("in1.discovery_prediction")
        ).as("discovery_prediction"),
        when(col("in1.campaign_group_type_id") =!= lit(0),
             col("in1.campaign_group_type_id")
        ).as("campaign_group_type_id"),
        col("in1.excluded_targeted_segment_details")
          .as("excluded_targeted_segment_details"),
        lit(null).cast(StringType).as("trust_id"),
        col("in1.predicted_kpi_event_rate").as("predicted_kpi_event_rate"),
        col("in1.has_crossdevice_reach_extension")
          .cast(BooleanType)
          .as("has_crossdevice_reach_extension"),
        lit(null).as("total_segment_data_costs_microcents"),
        lit(null).as("total_feature_costs_microcents"),
        col("in1.counterparty_ruleset_type").as("counterparty_ruleset_type"),
        col("in1.log_product_ads").as("log_product_ads"),
        when(col("in1.hb_source") =!= lit(0), col("in1.hb_source"))
          .as("hb_source"),
        coalesce(when((col("in1.imp_type") === lit(5))
                        .or(col("in1.imp_type") === lit(6))
                        .or(col("in1.imp_type") === lit(7)),
                      col("in1.buyer_line_item_currency")
                 ),
                 lit("---")
        ).as("buyer_line_item_currency"),
        coalesce(when((col("in1.imp_type") === lit(5))
                        .or(col("in1.imp_type") === lit(6))
                        .or(col("in1.imp_type") === lit(7)),
                      col("in1.deal_line_item_currency")
                 ),
                 lit("---")
        ).as("deal_line_item_currency"),
        col("in1.personal_identifiers_experimental")
          .as("personal_identifiers_experimental"),
        col("in1.postal_code_ext_id").as("postal_code_ext_id"),
        col("in1.targeted_segment_details_by_id_type")
          .as("targeted_segment_details_by_id_type"),
        col("in1.personal_identifiers").as("personal_identifiers"),
        col("in1.is_residential_ip").cast(BooleanType).as("is_residential_ip"),
        col("in1.hashed_ip").as("hashed_ip"),
        col("in1.district_postal_code_lists").as("district_postal_code_lists"),
        lit(null).cast(LongType).as("buyer_dpvp_bitmap"),
        lit(null).cast(LongType).as("seller_dpvp_bitmap"),
        col("in1.private_auction_eligible")
          .cast(BooleanType)
          .as("private_auction_eligible"),
        col("in1.chrome_traffic_label").as("chrome_traffic_label"),
        col("in1.is_private_auction")
          .cast(BooleanType)
          .as("is_private_auction"),
        col("in1.payment_type").as("ll1_payment_type"),
        col("in1.revenue_type").as("ll1_revenue_type"),
        col("in1.buyer_charges").as("ll1_buyer_charges"),
        col("in1.seller_charges").as("ll1_seller_charges"),
        col("in1.publisher_exchange_rate").as("ll1_publisher_exchange_rate"),
        col("in1.buyer_member_id").as("ll1_buyer_member_id"),
        col("in2.auction_event_pricing").as("ll2_auction_event_pricing"),
        col("in1.imp_type").as("ll1_imp_type"),
        col("in1.data_costs").as("ll1_data_costs"),
        col("in1.commission_revshare").as("ll1_commission_revshare"),
        col("in0.revenue_info").as("ll0_revenue_info"),
        col("in1.serving_fees_revshare").as("ll1_serving_fees_revshare"),
        col("in1.media_buy_rev_share_pct").as("ll1_media_buy_rev_share_pct")
      )

}
