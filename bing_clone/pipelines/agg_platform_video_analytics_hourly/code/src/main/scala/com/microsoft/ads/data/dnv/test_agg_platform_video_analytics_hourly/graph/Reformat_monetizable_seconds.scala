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

object Reformat_monetizable_seconds {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.select(key(context).as("key"),
              value(context).as("value"),
              col("pod_id_64").cast(LongType).as("pod_id_64"),
              col("pod_id_64_vector")
    )

  def key(context: Context) = {
    val spark  = context.spark
    val Config = context.config
    struct(
      col("key.ymdh").as("ymdh"),
      col("key.seller_member_id").cast(IntegerType).as("seller_member_id"),
      col("key.call_type").as("call_type"),
      col("key.publisher_id").cast(IntegerType).as("publisher_id"),
      col("key.site_id").cast(IntegerType).as("site_id"),
      col("key.tag_id").cast(IntegerType).as("tag_id"),
      col("key.browser_id").cast(IntegerType).as("browser_id"),
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
      col("key.billing_exchange_rate").as("billing_exchange_rate"),
      col("key.member_currency").as("member_currency"),
      col("key.member_exchange_rate").as("member_exchange_rate"),
      col("key.publisher_currency").as("publisher_currency"),
      col("key.publisher_exchange_rate").as("publisher_exchange_rate"),
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
      col("key.operating_system_id")
        .cast(IntegerType)
        .as("operating_system_id"),
      col("key.operating_system_family_id")
        .cast(IntegerType)
        .as("operating_system_family_id"),
      col("key.max_ad_duration").cast(IntegerType).as("max_ad_duration"),
      col("key.min_ad_duration").cast(IntegerType).as("min_ad_duration"),
      col("key.max_number_ads").cast(IntegerType).as("max_number_ads"),
      col("key.max_duration").cast(IntegerType).as("max_duration"),
      col("key.placement_set_id").cast(IntegerType).as("placement_set_id"),
      col("key.pod_has_bumpers").as("pod_has_bumpers"),
      col("key.video_content_duration")
        .cast(IntegerType)
        .as("video_content_duration"),
      col("key.fallback_ad_index").cast(IntegerType).as("fallback_ad_index"),
      col("key.pod_has_imps").as("pod_has_imps"),
      col("key.pod_has_responses").as("pod_has_responses"),
      col("key.region_id").cast(IntegerType).as("region_id"),
      col("key.pod_has_sold_imps").as("pod_has_sold_imps"),
      coalesce(
        when((col("key.pod_has_imps").cast(ByteType) === lit(0))
               .and(col("value.completions").cast(LongType) > lit(0)),
             lit(0)
        ),
        when((col("key.pod_has_imps").cast(ByteType) === lit(0))
               .and(col("value.completions").cast(LongType) === lit(0)),
             lit(1)
        ),
        when((col("key.pod_has_sold_imps").cast(ByteType) === lit(0))
               .and(col("value.completions").cast(LongType) === lit(0)),
             lit(2)
        ),
        when(
          ((col("value.imps_kept").cast(LongType) + col("value.imps_resold")
            .cast(LongType)).cast(IntegerType) === col("value.completions")
            .cast(LongType)).and(
            (col("value.impression_seconds_kept").cast(LongType) + col(
              "value.impression_seconds_resold"
            ).cast(LongType)).cast(IntegerType) === col("key.max_duration")
              .cast(IntegerType)
          ),
          lit(3)
        ),
        when(
          ((col("value.imps_kept").cast(LongType) + col("value.imps_resold")
            .cast(LongType)).cast(IntegerType) === col("key.max_number_ads")
            .cast(IntegerType)).and(
            (col("value.impression_seconds_kept").cast(LongType) + col(
              "value.impression_seconds_resold"
            ).cast(LongType)).cast(IntegerType) < col("key.max_duration")
              .cast(IntegerType)
          ),
          lit(4)
        ),
        when(
          ((col("value.imps_kept").cast(LongType) + col("value.imps_resold")
            .cast(LongType)).cast(IntegerType) === col("value.completions")
            .cast(LongType)).and(
            (col("value.impression_seconds_kept").cast(LongType) + col(
              "value.impression_seconds_resold"
            ).cast(LongType)).cast(IntegerType) < col("key.max_duration")
              .cast(IntegerType)
          ),
          lit(5)
        ),
        when((col("value.imps_kept").cast(LongType) + col("value.imps_resold")
               .cast(LongType)).cast(IntegerType) < col("value.completions")
               .cast(LongType),
             lit(6)
        ),
        when(
          ((col("value.imps_kept").cast(LongType) + col("value.imps_resold")
            .cast(LongType)).cast(IntegerType) > col("value.completions").cast(
            LongType
          )).and(col("value.imps_unsold").cast(LongType) > lit(0)),
          lit(7)
        ),
        when(
          ((col("value.imps_kept").cast(LongType) + col("value.imps_resold")
            .cast(LongType)).cast(IntegerType) > col("value.completions").cast(
            LongType
          )).and(col("value.imps_unsold").cast(LongType) === lit(0)),
          lit(8)
        ),
        lit(9).cast(IntegerType)
      ).as("pod_outcome"),
      col("key.pod_dh").as("pod_dh")
    )
  }

  def value(context: Context) = {
    val spark  = context.spark
    val Config = context.config
    struct(
      col("value.reseller_revenue_dollars").as("reseller_revenue_dollars"),
      col("value.booked_revenue_dollars").as("booked_revenue_dollars"),
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
      col("value.pod_count").cast(LongType).as("pod_count"),
      col("value.unmatched_min_slot_opportunities")
        .cast(LongType)
        .as("unmatched_min_slot_opportunities"),
      col("value.pod_no_responses").cast(LongType).as("pod_no_responses"),
      col("value.pod_no_imps").cast(LongType).as("pod_no_imps"),
      col("value.request_seconds").cast(LongType).as("request_seconds"),
      col("value.responses_seconds").cast(LongType).as("responses_seconds"),
      col("value.impression_seconds_resold")
        .cast(LongType)
        .as("impression_seconds_resold"),
      col("value.impression_seconds_kept")
        .cast(LongType)
        .as("impression_seconds_kept"),
      col("value.impression_seconds_unsold")
        .cast(LongType)
        .as("impression_seconds_unsold"),
      col("value.imps_resold").cast(LongType).as("imps_resold"),
      col("value.imps_kept").cast(LongType).as("imps_kept"),
      col("value.imps_unsold").cast(LongType).as("imps_unsold"),
      col("value.min_possible_opps").cast(LongType).as("min_possible_opps"),
      col("value.max_possible_opps").cast(LongType).as("max_possible_opps"),
      coalesce(
        when(
          col("value.impression_seconds_resold").cast(LongType) + col(
            "value.impression_seconds_kept"
          ).cast(LongType) === col("key.max_duration").cast(IntegerType),
          col("key.max_duration").cast(IntegerType)
        ),
        when(
          (col("value.completions")
            .cast(LongType) < (col("value.imps_kept").cast(LongType) + col(
            "value.imps_resold"
          ).cast(LongType)).cast(IntegerType)).and(
            (col("value.imps_kept").cast(LongType) + col("value.imps_resold")
              .cast(LongType)).cast(IntegerType) > lit(0)
          ),
          (col("value.impression_seconds_kept").cast(LongType) + col(
            "value.impression_seconds_resold"
          ).cast(LongType)).cast(IntegerType)
        ),
        when(
          (col("value.completions")
            .cast(LongType) === (col("value.imps_kept").cast(LongType) + col(
            "value.imps_resold"
          ).cast(LongType)).cast(IntegerType)).and(
            (col("value.imps_kept").cast(LongType) + col("value.imps_resold")
              .cast(LongType)).cast(IntegerType) > lit(0)
          ),
          col("key.max_duration").cast(IntegerType)
        ),
        when(
          (col("value.completions").cast(LongType) > (col("value.imps_kept")
            .cast(LongType) + col("value.imps_resold").cast(LongType)).cast(
            IntegerType
          )).and(col("key.pod_has_imps").cast(ByteType) === lit(1)),
          col("key.max_duration").cast(IntegerType)
        ),
        when((col("key.pod_has_imps").cast(ByteType) === lit(1))
               .and(col("key.pod_has_sold_imps").cast(ByteType) === lit(0)),
             col("key.max_duration").cast(IntegerType)
        ),
        when(col("key.pod_has_imps").cast(ByteType) === lit(0), lit(0)),
        lit(0).cast(LongType)
      ).as("fillable_duration"),
      when(
        col("value.impression_seconds_kept").cast(LongType) + col(
          "value.impression_seconds_resold"
        ).cast(LongType) === col("key.max_duration").cast(IntegerType),
        lit(0)
      ).when(
          (col("value.completions").cast(LongType) < col("value.imps_kept")
            .cast(LongType) + col("value.imps_resold").cast(LongType)).and(
            col("value.imps_kept").cast(LongType) + col("value.imps_resold")
              .cast(LongType) > lit(0)
          ),
          lit(0)
        )
        .when(
          (col("value.completions").cast(LongType) === col("value.imps_kept")
            .cast(LongType) + col("value.imps_resold").cast(LongType)).and(
            col("value.imps_kept").cast(LongType) + col("value.imps_resold")
              .cast(LongType) > lit(0)
          ),
          col("key.max_duration").cast(IntegerType) - (col(
            "value.impression_seconds_kept"
          ).cast(LongType) + col("value.impression_seconds_resold").cast(
            LongType
          )).cast(IntegerType)
        )
        .when(
          (col("value.completions").cast(LongType) > col("value.imps_kept")
            .cast(LongType) + col("value.imps_resold").cast(LongType))
            .and(col("key.pod_has_imps") === lit(1)),
          col("key.max_duration").cast(IntegerType) - (col(
            "value.impression_seconds_kept"
          ).cast(LongType) + col("value.impression_seconds_resold").cast(
            LongType
          )).cast(IntegerType)
        )
        .when((col("key.pod_has_imps") === lit(1)).and(
                col("key.pod_has_sold_imps") === lit(0)
              ),
              col("key.max_duration").cast(IntegerType)
        )
        .when(col("key.pod_has_imps") === lit(0), lit(0))
        .otherwise(lit(0))
        .cast(IntegerType)
        .as("unfilled_duration"),
      when(
        when(
          col("value.impression_seconds_kept").cast(LongType) + col(
            "value.impression_seconds_resold"
          ).cast(LongType) === col("key.max_duration").cast(IntegerType),
          lit(0)
        ).when(
            (col("value.completions").cast(LongType) < col("value.imps_kept")
              .cast(LongType) + col("value.imps_resold").cast(LongType)).and(
              col("value.imps_kept").cast(LongType) + col("value.imps_resold")
                .cast(LongType) > lit(0)
            ),
            lit(0)
          )
          .when(
            (col("value.completions").cast(LongType) === col("value.imps_kept")
              .cast(LongType) + col("value.imps_resold").cast(LongType)).and(
              col("value.imps_kept").cast(LongType) + col("value.imps_resold")
                .cast(LongType) > lit(0)
            ),
            col("key.max_duration").cast(IntegerType) - (col(
              "value.impression_seconds_kept"
            ).cast(LongType) + col("value.impression_seconds_resold").cast(
              LongType
            )).cast(IntegerType)
          )
          .when(
            (col("value.completions")
              .cast(LongType) > col("value.imps_kept").cast(LongType) + col(
              "value.imps_resold"
            ).cast(LongType)).and(col("key.pod_has_imps") === lit(1)),
            col("key.max_duration").cast(IntegerType) - (col(
              "value.impression_seconds_kept"
            ).cast(LongType) + col("value.impression_seconds_resold").cast(
              LongType
            )).cast(IntegerType)
          )
          .when((col("key.pod_has_imps") === lit(1)).and(
                  col("key.pod_has_sold_imps") === lit(0)
                ),
                col("key.max_duration").cast(IntegerType)
          )
          .when(col("key.pod_has_imps") === lit(0), lit(0))
          .otherwise(lit(0))
          .cast(IntegerType) < col("key.min_ad_duration").cast(IntegerType),
        when(
          col("value.impression_seconds_kept").cast(LongType) + col(
            "value.impression_seconds_resold"
          ).cast(LongType) === col("key.max_duration").cast(IntegerType),
          lit(0)
        ).when(
            (col("value.completions").cast(LongType) < col("value.imps_kept")
              .cast(LongType) + col("value.imps_resold").cast(LongType)).and(
              col("value.imps_kept").cast(LongType) + col("value.imps_resold")
                .cast(LongType) > lit(0)
            ),
            lit(0)
          )
          .when(
            (col("value.completions").cast(LongType) === col("value.imps_kept")
              .cast(LongType) + col("value.imps_resold").cast(LongType)).and(
              col("value.imps_kept").cast(LongType) + col("value.imps_resold")
                .cast(LongType) > lit(0)
            ),
            col("key.max_duration").cast(IntegerType) - (col(
              "value.impression_seconds_kept"
            ).cast(LongType) + col("value.impression_seconds_resold").cast(
              LongType
            )).cast(IntegerType)
          )
          .when(
            (col("value.completions")
              .cast(LongType) > col("value.imps_kept").cast(LongType) + col(
              "value.imps_resold"
            ).cast(LongType)).and(col("key.pod_has_imps") === lit(1)),
            col("key.max_duration").cast(IntegerType) - (col(
              "value.impression_seconds_kept"
            ).cast(LongType) + col("value.impression_seconds_resold").cast(
              LongType
            )).cast(IntegerType)
          )
          .when((col("key.pod_has_imps") === lit(1)).and(
                  col("key.pod_has_sold_imps") === lit(0)
                ),
                col("key.max_duration").cast(IntegerType)
          )
          .when(col("key.pod_has_imps") === lit(0), lit(0))
          .otherwise(lit(0))
          .cast(IntegerType)
      ).otherwise(lit(0)).as("unfilled_duration_below_min_ad_duration"),
      col("value.imps_sold_bumper").cast(LongType).as("imps_sold_bumper"),
      col("value.impression_seconds_sold_bumper")
        .cast(LongType)
        .as("impression_seconds_sold_bumper")
    )
  }

}
