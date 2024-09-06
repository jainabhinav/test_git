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

object Pre_Rollup_agg_platform_video_pod_analytics_pb_grp_by_key_partition_by_expr {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.groupBy(col("key"))
      .agg(value(context).as("value"),
           last(col("pod_id_64")).cast(LongType).as("pod_id_64"),
           last(col("pod_id_64_vector")).as("pod_id_64_vector")
      )

  def value(context: Context) = {
    val spark  = context.spark
    val Config = context.config
    struct(
      sum(col("value.reseller_revenue_dollars")).as("reseller_revenue_dollars"),
      sum(col("value.booked_revenue_dollars")).as("booked_revenue_dollars"),
      sum(col("value.starts").cast(LongType)).as("starts"),
      sum(col("value.skips").cast(LongType)).as("skips"),
      sum(col("value.errors").cast(LongType)).as("errors"),
      sum(col("value.first_quartiles").cast(LongType)).as("first_quartiles"),
      sum(col("value.second_quartiles").cast(LongType)).as("second_quartiles"),
      sum(col("value.third_quartiles").cast(LongType)).as("third_quartiles"),
      sum(col("value.completions").cast(LongType)).as("completions"),
      sum(col("value.clicks").cast(LongType)).as("clicks"),
      sum(col("value.ad_requests").cast(LongType)).as("ad_requests"),
      sum(col("value.ad_responses").cast(LongType)).as("ad_responses"),
      sum(col("value.pod_count").cast(LongType)).as("pod_count"),
      sum(col("value.unmatched_min_slot_opportunities").cast(LongType))
        .as("unmatched_min_slot_opportunities"),
      sum(col("value.pod_no_responses").cast(LongType)).as("pod_no_responses"),
      sum(col("value.pod_no_imps").cast(LongType)).as("pod_no_imps"),
      sum(col("value.request_seconds").cast(LongType)).as("request_seconds"),
      sum(col("value.responses_seconds").cast(LongType))
        .as("responses_seconds"),
      sum(col("value.impression_seconds_resold").cast(LongType))
        .as("impression_seconds_resold"),
      sum(col("value.impression_seconds_kept").cast(LongType))
        .as("impression_seconds_kept"),
      sum(col("value.impression_seconds_unsold").cast(LongType))
        .as("impression_seconds_unsold"),
      sum(col("value.imps_resold").cast(LongType)).as("imps_resold"),
      sum(col("value.imps_kept").cast(LongType)).as("imps_kept"),
      sum(col("value.imps_unsold").cast(LongType)).as("imps_unsold"),
      last(col("value.min_possible_opps"))
        .cast(LongType)
        .as("min_possible_opps"),
      last(col("value.max_possible_opps"))
        .cast(LongType)
        .as("max_possible_opps"),
      sum(col("value.fillable_duration").cast(LongType))
        .as("fillable_duration"),
      sum(col("value.unfilled_duration").cast(LongType))
        .as("unfilled_duration"),
      sum(col("value.unfilled_duration_below_min_ad_duration").cast(LongType))
        .as("unfilled_duration_below_min_ad_duration"),
      sum(col("value.imps_sold_bumper").cast(LongType)).as("imps_sold_bumper"),
      sum(col("value.impression_seconds_sold_bumper").cast(LongType))
        .as("impression_seconds_sold_bumper")
    )
  }

}
