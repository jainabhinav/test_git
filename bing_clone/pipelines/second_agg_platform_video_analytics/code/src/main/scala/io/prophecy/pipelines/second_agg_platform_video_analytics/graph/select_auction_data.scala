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

object select_auction_data {

  def apply(context: Context, in: DataFrame): DataFrame = {
    val Config = context.config
    in.select(
      col("auction_id_64"),
      col("date_time"),
      col("agg_platform_video_requests"),
      col("agg_dw_video_events"),
      col("agg_platform_video_impressions"),
      col("agg_dw_clicks"),
      col("agg_dw_pixels"),
      col("agg_impbus_clicks"),
      f_is_buy_side_imp(col("agg_dw_pixels.imp_type").cast(IntegerType))
        .cast(IntegerType)
        .as("f_is_buy_side_imp_agg_dw_pixels_imp_type"),
      f_is_buy_side_imp(
        coalesce(col("agg_dw_video_events.imp_type").cast(IntegerType),
                 col("agg_dw_video_events.request_imp_type").cast(IntegerType)
        )
      ).cast(IntegerType).as("f_is_buy_side_imp_imp_type_request_imp_type"),
      f_is_buy_side_imp(
        col("agg_platform_video_impressions.imp_type").cast(IntegerType)
      ).cast(IntegerType)
        .as("f_is_buy_side_imp_agg_platform_video_impressions_imp_type"),
      f_is_buy_side_imp(col("agg_dw_clicks.imp_type").cast(IntegerType))
        .cast(IntegerType)
        .as("f_is_buy_side_imp_agg_dw_clicks_imp_type"),
      f_calc_derived_fields(
        col("agg_dw_video_events"),
        col("agg_platform_video_impressions"),
        col("agg_platform_video_requests"),
        col("agg_dw_clicks"),
        col("agg_dw_pixels"),
        f_get_winning_creative_id(col("agg_dw_video_events"),
                                  col("agg_platform_video_impressions")
        ).cast(IntegerType),
        lit(Config.XR_BUSINESS_DATE),
        lit(Config.XR_BUSINESS_HOUR)
      ).getField("creative_id").cast(IntegerType).as("f_calc_derived_fields")
    )
  }

}
