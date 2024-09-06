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

object Reformat_agg_platform_video_slot_analytics_pb {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.select(
      col("key.ymdh").as("ymdh"),
      col("key.seller_member_id").cast(IntegerType).as("seller_member_id"),
      col("key.call_type").as("call_type"),
      col("key.publisher_id").cast(IntegerType).as("publisher_id"),
      col("key.site_id").cast(IntegerType).as("site_id"),
      col("key.tag_id").cast(IntegerType).as("tag_id"),
      col("key.application_id").as("application_id"),
      col("key.inventory_url_id").cast(IntegerType).as("inventory_url_id"),
      col("key.video_context").cast(IntegerType).as("video_context"),
      col("key.playback_method").cast(IntegerType).as("playback_method"),
      col("key.content_network_id").cast(IntegerType).as("content_network_id"),
      col("key.content_language_id")
        .cast(IntegerType)
        .as("content_language_id"),
      col("key.content_genre_id").cast(IntegerType).as("content_genre_id"),
      col("key.content_program_type_id")
        .cast(IntegerType)
        .as("content_program_type_id"),
      col("key.content_rating_id").cast(IntegerType).as("content_rating_id"),
      col("key.content_delivery_type_id")
        .cast(IntegerType)
        .as("content_delivery_type_id"),
      col("key.geo_country").as("geo_country"),
      col("key.billing_currency").as("billing_currency"),
      col("key.billing_exchange_rate")
        .cast(DoubleType)
        .as("billing_exchange_rate"),
      col("key.member_currency").as("member_currency"),
      col("key.member_exchange_rate")
        .cast(DoubleType)
        .as("member_exchange_rate"),
      col("key.publisher_currency").as("publisher_currency"),
      col("key.publisher_exchange_rate")
        .cast(DoubleType)
        .as("publisher_exchange_rate"),
      col("key.advertiser_default_currency").as("advertiser_default_currency"),
      col("key.advertiser_default_exchange_rate")
        .cast(DoubleType)
        .as("advertiser_default_exchange_rate"),
      col("key.creative_duration").cast(IntegerType).as("creative_duration"),
      col("key.creative_width").cast(IntegerType).as("creative_width"),
      col("key.creative_height").cast(IntegerType).as("creative_height"),
      col("key.device_type").cast(IntegerType).as("device_type"),
      col("key.supply_type").cast(IntegerType).as("supply_type"),
      col("key.language_id").cast(IntegerType).as("language_id"),
      col("key.player_width").cast(IntegerType).as("player_width"),
      col("key.player_height").cast(IntegerType).as("player_height"),
      col("key.supports_vpaid").cast(IntegerType).as("supports_vpaid"),
      col("key.max_vast_version").cast(IntegerType).as("max_vast_version"),
      col("key.city").cast(IntegerType).as("city"),
      col("key.content_category_id")
        .cast(IntegerType)
        .as("content_category_id"),
      col("key.placement_set_id").cast(IntegerType).as("placement_set_id"),
      col("key.video_content_duration")
        .cast(IntegerType)
        .as("video_content_duration"),
      col("key.buyer_member_id").cast(IntegerType).as("buyer_member_id"),
      col("key.bidder_id").cast(IntegerType).as("bidder_id"),
      col("key.advertiser_id").cast(IntegerType).as("advertiser_id"),
      col("key.insertion_order_id").cast(IntegerType).as("insertion_order_id"),
      col("key.campaign_group_id").cast(IntegerType).as("campaign_group_id"),
      col("key.creative_id").cast(IntegerType).as("creative_id"),
      col("key.brand_id").cast(IntegerType).as("brand_id"),
      col("key.deal_id").cast(IntegerType).as("deal_id"),
      col("key.imp_type").cast(IntegerType).as("imp_type"),
      col("key.ad_slot_position").cast(IntegerType).as("ad_slot_position"),
      col("key.slot_type").cast(IntegerType).as("slot_type"),
      col("key.buyer_seat_id").cast(IntegerType).as("buyer_seat_id"),
      col("key.external_deal_code").as("external_deal_code"),
      col("key.max_number_ads").cast(IntegerType).as("max_number_ads"),
      col("key.max_duration").cast(IntegerType).as("max_duration"),
      col("key.operating_system_id")
        .cast(IntegerType)
        .as("operating_system_id"),
      col("key.operating_system_family_id")
        .cast(IntegerType)
        .as("operating_system_family_id"),
      col("key.browser_id").cast(IntegerType).as("browser_id"),
      col("value.reseller_revenue_dollars")
        .cast(DoubleType)
        .as("reseller_revenue_dollars"),
      col("value.booked_revenue_dollars")
        .cast(DoubleType)
        .as("booked_revenue_dollars"),
      col("value.imps").cast(LongType).as("imps"),
      col("value.starts").cast(LongType).as("starts"),
      col("value.skips").cast(LongType).as("skips"),
      col("value.errors").cast(LongType).as("errors"),
      col("value.first_quartiles").cast(LongType).as("first_quartiles"),
      col("value.second_quartiles").cast(LongType).as("second_quartiles"),
      col("value.third_quartiles").cast(LongType).as("third_quartiles"),
      col("value.completions").cast(LongType).as("completions"),
      col("value.clicks").cast(LongType).as("clicks"),
      col("value.ad_requests").cast(LongType).as("ad_requests"),
      col("value.ad_responses").cast(LongType).as("ad_responses"),
      col("key.fallback_ad_index").cast(IntegerType).as("fallback_ad_index"),
      col("key.region_id").cast(IntegerType).as("region_id")
    )

}
