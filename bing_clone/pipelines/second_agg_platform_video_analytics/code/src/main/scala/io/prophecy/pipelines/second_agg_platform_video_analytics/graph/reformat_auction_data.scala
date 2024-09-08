package io.prophecy.pipelines.second_agg_platform_video_analytics.graph

import io.prophecy.libs._
import io.prophecy.pipelines.second_agg_platform_video_analytics.udfs.PipelineInitCode._
import io.prophecy.pipelines.second_agg_platform_video_analytics.udfs.UDFs._
import io.prophecy.pipelines.second_agg_platform_video_analytics.udfs.ColumnFunctions._
import io.prophecy.pipelines.second_agg_platform_video_analytics.config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object reformat_auction_data {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.select(
      col("auction_id_64"),
      col("agg_platform_video_requests.tag_id")
        .as("agg_platform_video_requests_tag_id"),
      col("agg_platform_video_requests.creative_id")
        .as("agg_platform_video_requests_creative_id"),
      col("agg_dw_video_events.creative_id")
        .as("agg_dw_video_events_creative_id"),
      col("agg_platform_video_impressions.creative_id")
        .as("agg_platform_video_impressions_creative_id"),
      col("agg_dw_clicks.creative_id").as("agg_dw_clicks_creative_id"),
      col("agg_dw_pixels.creative_id").as("agg_dw_pixels_creative_id"),
      col("agg_impbus_clicks.creative_id").as("agg_impbus_clicks_creative_id"),
      col("agg_dw_video_events.advertiser_id")
        .as("agg_dw_video_events_advertiser_id"),
      col("agg_platform_video_impressions.advertiser_id")
        .as("agg_platform_video_impressions_advertiser_id"),
      col("agg_dw_clicks.advertiser_id").as("agg_dw_clicks_advertiser_id"),
      col("agg_dw_pixels.advertiser_id").as("agg_dw_pixels_advertiser_id"),
      col("agg_impbus_clicks.advertiser_id")
        .as("agg_impbus_clicks_advertiser_id"),
      col("agg_dw_video_events.buyer_member_id")
        .as("agg_dw_video_events_buyer_member_id"),
      col("agg_platform_video_impressions.buyer_member_id")
        .as("agg_platform_video_impressions_buyer_member_id"),
      col("agg_dw_clicks.buyer_member_id").as("agg_dw_clicks_buyer_member_id"),
      col("agg_dw_pixels.buyer_member_id").as("agg_dw_pixels_buyer_member_id"),
      col("agg_impbus_clicks.buyer_member_id")
        .as("agg_impbus_clicks_buyer_member_id"),
      col("agg_platform_video_requests.seller_member_id")
        .as("agg_platform_video_requests_seller_member_id"),
      col("agg_dw_video_events.seller_member_id")
        .as("agg_dw_video_events_seller_member_id"),
      col("agg_platform_video_impressions.seller_member_id")
        .as("agg_platform_video_impressions_seller_member_id"),
      col("agg_dw_clicks.seller_member_id")
        .as("agg_dw_clicks_seller_member_id"),
      col("agg_dw_pixels.seller_member_id")
        .as("agg_dw_pixels_seller_member_id"),
      col("agg_impbus_clicks.seller_member_id")
        .as("agg_impbus_clicks_seller_member_id"),
      col("f_is_buy_side_imp_agg_dw_pixels_imp_type"),
      col("f_is_buy_side_imp_imp_type_request_imp_type"),
      col("f_is_buy_side_imp_agg_platform_video_impressions_imp_type"),
      col("f_is_buy_side_imp_agg_dw_clicks_imp_type"),
      col("f_calc_derived_fields")
    )

}
