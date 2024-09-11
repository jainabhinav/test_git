package io.prophecy.pipelines.second_agg_platform_video_analytics_hourly.graph

import io.prophecy.libs._
import io.prophecy.pipelines.second_agg_platform_video_analytics_hourly.udfs.PipelineInitCode._
import io.prophecy.pipelines.second_agg_platform_video_analytics_hourly.udfs.UDFs._
import io.prophecy.pipelines.second_agg_platform_video_analytics_hourly.udfs.ColumnFunctions._
import io.prophecy.pipelines.second_agg_platform_video_analytics_hourly.config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Pre_Rollup_agg_platform_video_pod_analytics_pb_grp_by_expr_partition_by_expr {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.groupBy(col("key.seller_member_id").as("seller_member_id"),
               col("pod_id_64").cast(LongType).as("pod_id_64")
      )
      .agg(key(context).as("key"),
           value(context).as("value"),
           last(col("pod_id_64_vector")).as("pod_id_64_vector")
      )

  def key(context: Context) = {
    val spark  = context.spark
    val Config = context.config
    struct(
      first(col("key.ymdh")).as("ymdh"),
      last(col("key.seller_member_id"))
        .cast(IntegerType)
        .as("seller_member_id"),
      first(col("key.call_type")).as("call_type"),
      first(col("key.publisher_id").cast(IntegerType)).as("publisher_id"),
      first(col("key.site_id").cast(IntegerType)).as("site_id"),
      first(col("key.tag_id").cast(IntegerType)).as("tag_id"),
      max(col("key.browser_id").cast(IntegerType)).as("browser_id"),
      first(col("key.application_id")).as("application_id"),
      first(col("key.inventory_url_id").cast(IntegerType))
        .as("inventory_url_id"),
      first(col("key.video_context").cast(IntegerType)).as("video_context"),
      max(col("key.playback_method").cast(IntegerType)).as("playback_method"),
      first(col("key.content_network_id").cast(IntegerType))
        .as("content_network_id"),
      first(col("key.content_language_id").cast(IntegerType))
        .as("content_language_id"),
      first(col("key.content_genre_id").cast(IntegerType))
        .as("content_genre_id"),
      first(col("key.content_program_type_id").cast(IntegerType))
        .as("content_program_type_id"),
      first(col("key.content_rating_id").cast(IntegerType))
        .as("content_rating_id"),
      first(col("key.content_delivery_type_id").cast(IntegerType))
        .as("content_delivery_type_id"),
      first(col("key.geo_country")).as("geo_country"),
      coalesce(max(
                 when(col("key.billing_currency") =!= lit("USD"),
                      col("key.billing_currency")
                 )
               ),
               lit("USD")
      ).as("billing_currency"),
      coalesce(max(
                 when(col("key.billing_exchange_rate") =!= lit(1.0d),
                      col("key.billing_exchange_rate")
                 )
               ),
               lit(1.0d)
      ).as("billing_exchange_rate"),
      coalesce(max(
                 when(col("key.member_currency") =!= lit("USD"),
                      col("key.member_currency")
                 )
               ),
               lit("USD")
      ).as("member_currency"),
      coalesce(max(
                 when(col("key.member_exchange_rate") =!= lit(1.0d),
                      col("key.member_exchange_rate")
                 )
               ),
               lit(1.0d)
      ).as("member_exchange_rate"),
      coalesce(max(
                 when(col("key.publisher_currency") =!= lit("USD"),
                      col("key.publisher_currency")
                 )
               ),
               lit("USD")
      ).as("publisher_currency"),
      coalesce(max(
                 when(col("key.publisher_exchange_rate") =!= lit(1.0d),
                      col("key.publisher_exchange_rate")
                 )
               ),
               lit(1.0d)
      ).as("publisher_exchange_rate"),
      first(col("key.device_type").cast(IntegerType)).as("device_type"),
      first(col("key.supply_type").cast(IntegerType)).as("supply_type"),
      first(col("key.language_id").cast(IntegerType)).as("language_id"),
      first(col("key.player_width").cast(IntegerType)).as("player_width"),
      first(col("key.player_height").cast(IntegerType)).as("player_height"),
      first(col("key.supports_vpaid").cast(IntegerType)).as("supports_vpaid"),
      first(col("key.max_vast_version").cast(IntegerType))
        .as("max_vast_version"),
      max(col("key.city").cast(IntegerType)).as("city"),
      max(col("key.content_category_id").cast(IntegerType))
        .as("content_category_id"),
      max(col("key.operating_system_id").cast(IntegerType))
        .as("operating_system_id"),
      first(col("key.operating_system_family_id").cast(IntegerType))
        .as("operating_system_family_id"),
      coalesce(
        max(
          when(not(col("key.pod_has_bumpers").cast(BooleanType)),
               col("key.max_ad_duration").cast(IntegerType)
          )
        ),
        max(col("key.max_ad_duration").cast(IntegerType))
      ).as("max_ad_duration"),
      first(col("key.min_ad_duration").cast(IntegerType)).as("min_ad_duration"),
      first(col("key.max_number_ads").cast(IntegerType)).as("max_number_ads"),
      first(col("key.max_duration").cast(IntegerType)).as("max_duration"),
      first(col("key.placement_set_id").cast(IntegerType))
        .as("placement_set_id"),
      max(col("key.pod_has_bumpers")).cast(BooleanType).as("pod_has_bumpers"),
      first(col("key.video_content_duration").cast(IntegerType))
        .as("video_content_duration"),
      max(col("key.fallback_ad_index").cast(IntegerType))
        .as("fallback_ad_index"),
      (sum(col("value.imps_resold").cast(LongType)) > lit(0))
        .or(sum(col("value.imps_kept").cast(LongType)) > lit(0))
        .or(sum(col("value.imps_unsold").cast(LongType)) > lit(0))
        .and(max(col("key.pod_has_bumpers")) === lit(0))
        .as("pod_has_imps"),
      (sum(col("value.ad_responses").cast(LongType)) > lit(0))
        .as("pod_has_responses"),
      max(col("key.region_id").cast(IntegerType)).as("region_id"),
      (sum(col("value.imps_resold").cast(LongType)) > lit(0))
        .or(sum(col("value.imps_kept").cast(LongType)) > lit(0))
        .and(max(col("key.pod_has_bumpers")) === lit(0))
        .as("pod_has_sold_imps"),
      last(col("key.pod_outcome")).cast(IntegerType).as("pod_outcome"),
      last(col("key.pod_dh")).as("pod_dh")
    )
  }

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
      last(lit(1)).as("pod_count"),
      floor(
        (first(col("key.max_duration").cast(IntegerType)) - sum(
          col("value.impression_seconds_kept").cast(LongType) + col(
            "value.impression_seconds_resold"
          ).cast(LongType) + col("value.impression_seconds_unsold")
            .cast(LongType)
        )) / when(
          coalesce(
            max(
              when(not(col("key.pod_has_bumpers").cast(BooleanType)),
                   col("key.max_ad_duration").cast(IntegerType)
              )
            ),
            max(col("key.max_ad_duration").cast(IntegerType))
          ) > lit(0),
          coalesce(
            max(
              when(not(col("key.pod_has_bumpers").cast(BooleanType)),
                   col("key.max_ad_duration").cast(IntegerType)
              )
            ),
            max(col("key.max_ad_duration").cast(IntegerType))
          )
        ).otherwise(lit(1))
      ).as("unmatched_min_slot_opportunities"),
      (sum(col("value.ad_responses").cast(LongType)) === lit(0))
        .as("pod_no_responses"),
      (sum(col("value.imps_resold").cast(LongType)) === lit(0))
        .and(sum(col("value.imps_kept").cast(LongType)) === lit(0))
        .and(sum(col("value.imps_unsold").cast(LongType)) === lit(0))
        .as("pod_no_imps"),
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
      last(col("value.fillable_duration"))
        .cast(LongType)
        .as("fillable_duration"),
      last(col("value.unfilled_duration"))
        .cast(LongType)
        .as("unfilled_duration"),
      last(col("value.unfilled_duration_below_min_ad_duration"))
        .cast(LongType)
        .as("unfilled_duration_below_min_ad_duration"),
      sum(col("value.imps_sold_bumper").cast(LongType)).as("imps_sold_bumper"),
      sum(col("value.impression_seconds_sold_bumper").cast(LongType))
        .as("impression_seconds_sold_bumper")
    )
  }

}
