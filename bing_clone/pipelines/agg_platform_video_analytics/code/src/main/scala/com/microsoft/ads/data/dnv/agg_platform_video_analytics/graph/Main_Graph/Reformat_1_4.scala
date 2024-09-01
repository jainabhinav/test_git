package com.microsoft.ads.data.dnv.agg_platform_video_analytics.graph.Main_Graph

import io.prophecy.libs._
import com.microsoft.ads.data.dnv.agg_platform_video_analytics.udfs.PipelineInitCode._
import com.microsoft.ads.data.dnv.agg_platform_video_analytics.udfs.UDFs._
import com.microsoft.ads.data.dnv.agg_platform_video_analytics.udfs.ColumnFunctions._
import com.microsoft.ads.data.dnv.agg_platform_video_analytics.graph.Main_Graph.config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Reformat_1_4 {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.select(
      col("date_time"),
      col("auction_id_64"),
      col("tag_id"),
      col("width"),
      col("height"),
      col("geo_country"),
      when(col("geo_region") === lit("--"), lit(null).cast(StringType))
        .otherwise(col("geo_region"))
        .as("geo_region"),
      col("seller_member_id"),
      col("imp_blacklist_or_fraud"),
      col("call_type"),
      col("brand_id"),
      col("imp_type"),
      col("publisher_id"),
      col("site_id"),
      col("content_category_id"),
      col("media_type"),
      col("browser"),
      col("language"),
      col("application_id"),
      col("city"),
      col("supply_type"),
      col("deal_id"),
      when(col("vp_bitmap") === lit(0), lit(null).cast(LongType))
        .otherwise(col("vp_bitmap"))
        .as("vp_bitmap"),
      col("device_type"),
      col("view_detection_enabled"),
      when(col("viewdef_definition_id") === 0, lit(null).cast(IntegerType))
        .otherwise(col("viewdef_definition_id"))
        .as("viewdef_definition_id"),
      col("request_uuid"),
      when(col("operating_system_id") === lit(1), lit(null).cast(IntegerType))
        .otherwise(col("operating_system_id"))
        .as("operating_system_id"),
      col("operating_system_family_id"),
      col("allowed_media_types"),
      col("is_imp_rejecter_applied"),
      col("imp_rejecter_do_auction"),
      col("imp_bid_on"),
      col("is_prebid"),
      col("placement_set_id"),
      col("max_duration"),
      col("max_number_ads"),
      col("supported_playback_methods"),
      col("video_context"),
      col("player_width"),
      col("player_height"),
      col("start_delay"),
      col("frameworks"),
      col("protocols"),
      col("supports_skippable"),
      col("min_ad_duration"),
      col("max_ad_duration"),
      col("slot_type"),
      col("ad_slot_position"),
      col("inventory_url_id"),
      col("is_dw_seller"),
      col("is_mediated"),
      col("num_of_bids"),
      col("buyer_bid"),
      col("creative_id"),
      col("filled_number_ads"),
      col("max_slot_duration"),
      col("filled_slot_duration"),
      when(col("mobile_app_instance_id") === 0, lit(null).cast(IntegerType))
        .otherwise(col("mobile_app_instance_id"))
        .as("mobile_app_instance_id"),
      col("user_id_64"),
      col("instance_id"),
      when(col("hb_source") === lit(0), lit(null).cast(IntegerType))
        .otherwise(col("hb_source"))
        .as("hb_source"),
      col("content_delivery_type_id"),
      col("content_duration_secs"),
      col("content_genre_id"),
      col("content_program_type_id"),
      col("content_rating_id"),
      col("content_network_id"),
      col("content_language_id"),
      col("external_deal_code"),
      col("creative_duration"),
      col("pod_id_64"),
      col("imp_requests"),
      col("region_id")
    )

}
