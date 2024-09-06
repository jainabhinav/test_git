package com.microsoft.ads.data.dnv.test_agg_platform_video_analytics_hourly.graph

import io.prophecy.libs._
import com.microsoft.ads.data.dnv.test_agg_platform_video_analytics_hourly.udfs.PipelineInitCode._
import com.microsoft.ads.data.dnv.test_agg_platform_video_analytics_hourly.udfs.UDFs._
import com.microsoft.ads.data.dnv.test_agg_platform_video_analytics_hourly.udfs.ColumnFunctions._
import com.microsoft.ads.data.dnv.test_agg_platform_video_analytics_hourly.config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Reformat_agg_platform_video_analytics_hourly_pb {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.select(
      col("key.ymdh").as("ymdh"),
      col("key.imp_type").cast(IntegerType).as("imp_type"),
      col("key.is_dw_buyer").cast(IntegerType).as("is_dw_buyer"),
      col("key.is_dw_seller").cast(IntegerType).as("is_dw_seller"),
      col("key.seller_member_id").cast(IntegerType).as("seller_member_id"),
      col("key.publisher_id").cast(IntegerType).as("publisher_id"),
      col("key.site_id").cast(IntegerType).as("site_id"),
      col("key.tag_id").cast(IntegerType).as("tag_id"),
      col("key.placement_set_id").cast(IntegerType).as("placement_set_id"),
      col("key.site_domain").as("site_domain"),
      col("key.call_type").as("call_type"),
      col("key.is_prebid").cast(BooleanType).as("is_prebid"),
      col("key.is_super_auction").cast(BooleanType).as("is_super_auction"),
      lit(0).cast(BooleanType).as("is_mediated"),
      col("key.deal_id").cast(IntegerType).as("deal_id"),
      col("key.content_category_id")
        .cast(IntegerType)
        .as("content_category_id"),
      col("key.playback_method").cast(IntegerType).as("playback_method"),
      col("key.video_context").cast(IntegerType).as("video_context"),
      col("key.player_width").cast(IntegerType).as("player_width"),
      col("key.player_height").cast(IntegerType).as("player_height"),
      col("key.max_duration").cast(IntegerType).as("max_duration"),
      col("key.start_delay").cast(IntegerType).as("start_delay"),
      col("key.supports_vpaid").cast(IntegerType).as("supports_vpaid"),
      col("key.max_vast_version").cast(IntegerType).as("max_vast_version"),
      col("key.supports_skippable").cast(BooleanType).as("supports_skippable"),
      col("key.min_ad_duration").cast(IntegerType).as("min_ad_duration"),
      col("key.max_ad_duration").cast(IntegerType).as("max_ad_duration"),
      col("key.buyer_member_id").cast(IntegerType).as("buyer_member_id"),
      col("key.advertiser_id").cast(IntegerType).as("advertiser_id"),
      col("key.insertion_order_id").cast(IntegerType).as("insertion_order_id"),
      col("key.campaign_group_id").cast(IntegerType).as("campaign_group_id"),
      col("key.campaign_id").cast(IntegerType).as("campaign_id"),
      col("key.bidder_id").cast(IntegerType).as("bidder_id"),
      col("key.media_type").cast(IntegerType).as("media_type"),
      col("key.brand_id").cast(IntegerType).as("brand_id"),
      col("key.creative_id").cast(IntegerType).as("creative_id"),
      col("key.vast_error_code").cast(IntegerType).as("vast_error_code"),
      col("key.creative_duration").cast(IntegerType).as("creative_duration"),
      col("key.viewdef_definition_id")
        .cast(IntegerType)
        .as("viewdef_definition_id"),
      col("key.companion_creative_id")
        .cast(IntegerType)
        .as("companion_creative_id"),
      col("key.view_detection_enabled")
        .cast(IntegerType)
        .as("view_detection_enabled"),
      col("key.device_type").cast(IntegerType).as("device_type"),
      col("key.supply_type").cast(IntegerType).as("supply_type"),
      col("key.geo_country").as("geo_country"),
      col("value.booked_revenue_dollars")
        .cast(DoubleType)
        .as("booked_revenue_dollars"),
      col("value.reseller_revenue_dollars")
        .cast(DoubleType)
        .as("reseller_revenue_dollars"),
      col("value.media_cost_dollars").cast(DoubleType).as("media_cost_dollars"),
      col("value.booked_revenue_adv_currency")
        .cast(DoubleType)
        .as("booked_revenue_adv_currency"),
      col("value.media_cost_pub_currency")
        .cast(DoubleType)
        .as("media_cost_pub_currency"),
      col("key.advertiser_currency").as("advertiser_currency"),
      col("key.publisher_currency").as("publisher_currency"),
      col("value.ad_requests").cast(LongType).as("ad_requests"),
      col("value.blacklisted_imps").cast(LongType).as("blacklisted_imps"),
      col("value.ad_responses").cast(LongType).as("ad_responses"),
      col("value.imps").cast(LongType).as("imps"),
      col("value.starts").cast(LongType).as("starts"),
      col("value.skips").cast(LongType).as("skips"),
      col("value.errors").cast(LongType).as("errors"),
      col("value.first_quartiles").cast(LongType).as("first_quartiles"),
      col("value.second_quartiles").cast(LongType).as("second_quartiles"),
      col("value.third_quartiles").cast(LongType).as("third_quartiles"),
      col("value.completions").cast(LongType).as("completions"),
      col("value.clicks").cast(LongType).as("clicks"),
      col("value.post_click_conversions")
        .cast(LongType)
        .as("post_click_conversions"),
      col("value.post_view_conversions")
        .cast(LongType)
        .as("post_view_conversions"),
      col("value.companion_imps").cast(LongType).as("companion_imps"),
      col("value.companion_clicks").cast(LongType).as("companion_clicks"),
      col("value.viewed_imps").cast(LongType).as("viewed_imps"),
      col("value.view_measurable_imps")
        .cast(LongType)
        .as("view_measurable_imps"),
      col("key.application_id").as("application_id"),
      col("key.wrapper_vast_error_code")
        .cast(IntegerType)
        .as("wrapper_vast_error_code"),
      col("key.wrapper_internal_error_code")
        .cast(IntegerType)
        .as("wrapper_internal_error_code"),
      col("value.imps_bid_on").cast(LongType).as("imps_bid_on"),
      col("value.num_of_bids").cast(LongType).as("num_of_bids"),
      col("value.buyer_bid").cast(DoubleType).as("sum_of_bids"),
      col("key.seller_member_group_id")
        .cast(IntegerType)
        .as("seller_member_group_id"),
      col("key.is_vpaid").cast(BooleanType).as("is_vpaid"),
      col("value.viewdef_viewed_imps").cast(LongType).as("viewdef_viewed_imps"),
      col("key.ad_slot_position").cast(IntegerType).as("ad_slot_position"),
      col("key.slot_type").cast(IntegerType).as("slot_type"),
      col("key.max_number_ads").cast(IntegerType).as("max_number_ads"),
      math_min(lit(2147483647), col("filled_number_ads_64").cast(LongType))
        .cast(IntegerType)
        .as("filled_number_ads"),
      math_min(lit(2147483647), col("max_slot_duration_64").cast(LongType))
        .cast(IntegerType)
        .as("max_slot_duration"),
      math_min(lit(2147483647), col("filled_slot_duration_64").cast(LongType))
        .cast(IntegerType)
        .as("filled_slot_duration"),
      col("key.url_list").cast(IntegerType).as("url_list"),
      col("value.biddable_imps").cast(LongType).as("biddable_imps"),
      col("key.seller_revenue_event_type_id")
        .cast(IntegerType)
        .as("seller_revenue_event_type_id"),
      col("value.seller_revenue_event_units")
        .cast(LongType)
        .as("seller_revenue_event_units"),
      col("key.buyer_payment_event_type_id")
        .cast(IntegerType)
        .as("buyer_payment_event_type_id"),
      col("value.buyer_payment_event_units")
        .cast(LongType)
        .as("buyer_payment_event_units"),
      col("key.allowed_media_types_bitmap")
        .cast(IntegerType)
        .as("allowed_media_types_bitmap"),
      col("key.protocols_bitmap").cast(IntegerType).as("protocols_bitmap"),
      col("key.frameworks_bitmap").cast(IntegerType).as("frameworks_bitmap"),
      col("key.inventory_url_id").cast(IntegerType).as("inventory_url_id"),
      col("key.mobile_app_instance_id")
        .cast(IntegerType)
        .as("mobile_app_instance_id"),
      col("key.split_id").cast(IntegerType).as("split_id"),
      col("key.campaign_group_type_id")
        .cast(IntegerType)
        .as("campaign_group_type_id"),
      col("key.advertiser_default_currency").as("advertiser_default_currency"),
      col("key.advertiser_default_exchange_rate")
        .cast(DoubleType)
        .as("advertiser_default_exchange_rate"),
      col("key.member_currency").as("member_currency"),
      col("key.member_exchange_rate")
        .cast(DoubleType)
        .as("member_exchange_rate"),
      col("key.billing_currency").as("billing_currency"),
      col("key.billing_exchange_rate")
        .cast(DoubleType)
        .as("billing_exchange_rate"),
      lit(null).cast(IntegerType).as("fx_rate_snapshot_id"),
      col("key.hb_source").cast(IntegerType).as("hb_source"),
      col("key.bidder_seat_id").cast(IntegerType).as("bidder_seat_id"),
      col("key.content_delivery_type_id")
        .cast(IntegerType)
        .as("content_delivery_type_id"),
      col("key.content_duration_secs")
        .cast(IntegerType)
        .as("content_duration_secs"),
      col("key.content_genre_id").cast(IntegerType).as("content_genre_id"),
      col("key.content_program_type_id")
        .cast(IntegerType)
        .as("content_program_type_id"),
      col("key.content_rating_id").cast(IntegerType).as("content_rating_id"),
      col("key.content_network_id").cast(IntegerType).as("content_network_id"),
      col("key.content_language_id")
        .cast(IntegerType)
        .as("content_language_id"),
      lit(null).cast(LongType).as("imp_requests"),
      col("key.fallback_ad_index").cast(IntegerType).as("fallback_ad_index")
    )

}
