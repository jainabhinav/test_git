package io.prophecy.pipelines.first_agg_platform_video_analytics_hourly.graph

import io.prophecy.libs._
import io.prophecy.pipelines.first_agg_platform_video_analytics_hourly.udfs.PipelineInitCode._
import io.prophecy.pipelines.first_agg_platform_video_analytics_hourly.udfs.UDFs._
import io.prophecy.pipelines.first_agg_platform_video_analytics_hourly.udfs.ColumnFunctions._
import io.prophecy.pipelines.first_agg_platform_video_analytics_hourly.config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Pre_Rollup_agg_platform_video_analytics_hourly_pb {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.groupBy(col("key"))
      .agg(
        value(context).as("value"),
        sum(
          when(is_not_null(col("filled_number_ads_64").cast(LongType))
                 .cast(BooleanType),
               col("filled_number_ads_64").cast(LongType)
          )
        ).as("filled_number_ads_64"),
        sum(
          when(is_not_null(col("max_slot_duration_64").cast(LongType))
                 .cast(BooleanType),
               col("max_slot_duration_64").cast(LongType)
          )
        ).as("max_slot_duration_64"),
        sum(
          when(is_not_null(col("filled_slot_duration_64").cast(LongType))
                 .cast(BooleanType),
               col("filled_slot_duration_64").cast(LongType)
          )
        ).as("filled_slot_duration_64")
      )

  def value(context: Context) = {
    val spark  = context.spark
    val Config = context.config
    struct(
      sum(
        when(is_not_null(col("value.booked_revenue_dollars")).cast(BooleanType),
             col("value.booked_revenue_dollars")
        )
      ).as("booked_revenue_dollars"),
      sum(
        when(
          is_not_null(col("value.reseller_revenue_dollars")).cast(BooleanType),
          col("value.reseller_revenue_dollars")
        )
      ).as("reseller_revenue_dollars"),
      sum(
        when(is_not_null(col("value.media_cost_dollars")).cast(BooleanType),
             col("value.media_cost_dollars")
        )
      ).as("media_cost_dollars"),
      sum(
        when(is_not_null(col("value.booked_revenue_adv_currency"))
               .cast(BooleanType),
             col("value.booked_revenue_adv_currency")
        )
      ).as("booked_revenue_adv_currency"),
      sum(
        when(
          is_not_null(col("value.media_cost_pub_currency")).cast(BooleanType),
          col("value.media_cost_pub_currency")
        )
      ).as("media_cost_pub_currency"),
      sum(
        when(is_not_null(col("value.ad_requests").cast(LongType))
               .cast(BooleanType),
             col("value.ad_requests").cast(LongType)
        )
      ).as("ad_requests"),
      sum(
        when(is_not_null(col("value.blacklisted_imps").cast(LongType))
               .cast(BooleanType),
             col("value.blacklisted_imps").cast(LongType)
        )
      ).as("blacklisted_imps"),
      sum(
        when(is_not_null(col("value.ad_responses").cast(LongType))
               .cast(BooleanType),
             col("value.ad_responses").cast(LongType)
        )
      ).as("ad_responses"),
      sum(
        when(is_not_null(col("value.imps").cast(LongType)).cast(BooleanType),
             col("value.imps").cast(LongType)
        )
      ).as("imps"),
      sum(
        when(is_not_null(col("value.starts").cast(LongType)).cast(BooleanType),
             col("value.starts").cast(LongType)
        )
      ).as("starts"),
      sum(
        when(is_not_null(col("value.skips").cast(LongType)).cast(BooleanType),
             col("value.skips").cast(LongType)
        )
      ).as("skips"),
      sum(
        when(is_not_null(col("value.errors").cast(LongType)).cast(BooleanType),
             col("value.errors").cast(LongType)
        )
      ).as("errors"),
      sum(
        when(is_not_null(col("value.first_quartiles").cast(LongType))
               .cast(BooleanType),
             col("value.first_quartiles").cast(LongType)
        )
      ).as("first_quartiles"),
      sum(
        when(is_not_null(col("value.second_quartiles").cast(LongType))
               .cast(BooleanType),
             col("value.second_quartiles").cast(LongType)
        )
      ).as("second_quartiles"),
      sum(
        when(is_not_null(col("value.third_quartiles").cast(LongType))
               .cast(BooleanType),
             col("value.third_quartiles").cast(LongType)
        )
      ).as("third_quartiles"),
      sum(
        when(is_not_null(col("value.completions").cast(LongType))
               .cast(BooleanType),
             col("value.completions").cast(LongType)
        )
      ).as("completions"),
      sum(
        when(is_not_null(col("value.clicks").cast(LongType)).cast(BooleanType),
             col("value.clicks").cast(LongType)
        )
      ).as("clicks"),
      sum(
        when(is_not_null(col("value.post_click_conversions").cast(LongType))
               .cast(BooleanType),
             col("value.post_click_conversions").cast(LongType)
        )
      ).as("post_click_conversions"),
      sum(
        when(is_not_null(col("value.post_view_conversions").cast(LongType))
               .cast(BooleanType),
             col("value.post_view_conversions").cast(LongType)
        )
      ).as("post_view_conversions"),
      sum(
        when(is_not_null(col("value.companion_imps").cast(LongType))
               .cast(BooleanType),
             col("value.companion_imps").cast(LongType)
        )
      ).as("companion_imps"),
      sum(
        when(is_not_null(col("value.companion_clicks").cast(LongType))
               .cast(BooleanType),
             col("value.companion_clicks").cast(LongType)
        )
      ).as("companion_clicks"),
      sum(
        when(is_not_null(col("value.viewed_imps").cast(LongType))
               .cast(BooleanType),
             col("value.viewed_imps").cast(LongType)
        )
      ).as("viewed_imps"),
      sum(
        when(is_not_null(col("value.view_measurable_imps").cast(LongType))
               .cast(BooleanType),
             col("value.view_measurable_imps").cast(LongType)
        )
      ).as("view_measurable_imps"),
      sum(
        when(is_not_null(col("value.viewdef_viewed_imps").cast(LongType))
               .cast(BooleanType),
             col("value.viewdef_viewed_imps").cast(LongType)
        )
      ).as("viewdef_viewed_imps"),
      sum(
        when(is_not_null(col("value.imps_bid_on").cast(LongType))
               .cast(BooleanType),
             col("value.imps_bid_on").cast(LongType)
        )
      ).as("imps_bid_on"),
      sum(
        when(is_not_null(col("value.num_of_bids").cast(LongType))
               .cast(BooleanType),
             col("value.num_of_bids").cast(LongType)
        )
      ).as("num_of_bids"),
      sum(
        when(is_not_null(col("value.buyer_bid")).cast(BooleanType),
             col("value.buyer_bid")
        )
      ).as("buyer_bid"),
      last(col("value.filled_number_ads"))
        .cast(IntegerType)
        .as("filled_number_ads"),
      last(col("value.max_slot_duration"))
        .cast(IntegerType)
        .as("max_slot_duration"),
      last(col("value.filled_slot_duration"))
        .cast(IntegerType)
        .as("filled_slot_duration"),
      sum(
        when(is_not_null(col("value.biddable_imps").cast(LongType))
               .cast(BooleanType),
             col("value.biddable_imps").cast(LongType)
        )
      ).as("biddable_imps"),
      sum(
        when(is_not_null(col("value.buyer_payment_event_units").cast(LongType))
               .cast(BooleanType),
             col("value.buyer_payment_event_units").cast(LongType)
        )
      ).as("buyer_payment_event_units"),
      sum(
        when(is_not_null(col("value.seller_revenue_event_units").cast(LongType))
               .cast(BooleanType),
             col("value.seller_revenue_event_units").cast(LongType)
        )
      ).as("seller_revenue_event_units"),
      last(col("value.imp_requests")).cast(LongType).as("imp_requests")
    )
  }

}
