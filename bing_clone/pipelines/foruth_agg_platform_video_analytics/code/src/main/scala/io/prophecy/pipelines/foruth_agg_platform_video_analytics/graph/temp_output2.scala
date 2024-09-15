package io.prophecy.pipelines.foruth_agg_platform_video_analytics.graph

import io.prophecy.libs._
import io.prophecy.pipelines.foruth_agg_platform_video_analytics.config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object temp_output2 {

  def apply(context: Context): DataFrame =
    context.spark.read
      .format("csv")
      .option("header", true)
      .option("sep",    ",")
      .schema(
        StructType(
          Array(
            StructField("auction_id_64", LongType, true),
            StructField("agg_platform_video_requests_tag_id",
                        IntegerType,
                        true
            ),
            StructField("agg_platform_video_requests_creative_id",
                        IntegerType,
                        true
            ),
            StructField("agg_dw_video_events_creative_id", IntegerType, true),
            StructField("agg_platform_video_impressions_creative_id",
                        IntegerType,
                        true
            ),
            StructField("agg_dw_clicks_creative_id",         IntegerType, true),
            StructField("agg_dw_pixels_creative_id",         IntegerType, true),
            StructField("agg_impbus_clicks_creative_id",     IntegerType, true),
            StructField("agg_dw_video_events_advertiser_id", IntegerType, true),
            StructField("agg_platform_video_impressions_advertiser_id",
                        IntegerType,
                        true
            ),
            StructField("agg_dw_clicks_advertiser_id",     IntegerType, true),
            StructField("agg_dw_pixels_advertiser_id",     IntegerType, true),
            StructField("agg_impbus_clicks_advertiser_id", IntegerType, true),
            StructField("agg_dw_video_events_buyer_member_id",
                        IntegerType,
                        true
            ),
            StructField("agg_platform_video_impressions_buyer_member_id",
                        IntegerType,
                        true
            ),
            StructField("agg_dw_clicks_buyer_member_id",     IntegerType, true),
            StructField("agg_dw_pixels_buyer_member_id",     IntegerType, true),
            StructField("agg_impbus_clicks_buyer_member_id", IntegerType, true),
            StructField("agg_platform_video_requests_seller_member_id",
                        IntegerType,
                        true
            ),
            StructField("agg_dw_video_events_seller_member_id",
                        IntegerType,
                        true
            ),
            StructField("agg_platform_video_impressions_seller_member_id",
                        IntegerType,
                        true
            ),
            StructField("agg_dw_clicks_seller_member_id", IntegerType, true),
            StructField("agg_dw_pixels_seller_member_id", IntegerType, true),
            StructField("agg_impbus_clicks_seller_member_id",
                        IntegerType,
                        true
            ),
            StructField("f_is_buy_side_imp_agg_dw_pixels_imp_type",
                        IntegerType,
                        true
            ),
            StructField("f_is_buy_side_imp_imp_type_request_imp_type",
                        IntegerType,
                        true
            ),
            StructField(
              "f_is_buy_side_imp_agg_platform_video_impressions_imp_type",
              IntegerType,
              true
            ),
            StructField("f_is_buy_side_imp_agg_dw_clicks_imp_type",
                        IntegerType,
                        true
            ),
            StructField("f_calc_derived_fields", IntegerType, true)
          )
        )
      )
      .load("asd")

}
