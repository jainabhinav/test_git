package io.prophecy.pipelines.first_agg_platform_video_analytics.graph

import io.prophecy.libs._
import io.prophecy.pipelines.first_agg_platform_video_analytics.udfs.PipelineInitCode._
import io.prophecy.pipelines.first_agg_platform_video_analytics.udfs.UDFs._
import io.prophecy.pipelines.first_agg_platform_video_analytics.udfs.ColumnFunctions._
import io.prophecy.pipelines.first_agg_platform_video_analytics.config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object join_auction_data {

  def apply(context: Context, in0: DataFrame, in1: DataFrame): DataFrame =
    in0
      .as("in0")
      .join(in1.as("in1"),
            col("in0.auction_id_64") === col("in1.auction_id_64"),
            "inner"
      )
      .select(
        col("in1._sup_bidder_advertiser_pb_LOOKUP1")
          .as("_sup_bidder_advertiser_pb_LOOKUP1"),
        col("in1._sup_bidder_advertiser_pb_LOOKUP5")
          .as("_sup_bidder_advertiser_pb_LOOKUP5"),
        col("in1._sup_bidder_advertiser_pb_LOOKUP7")
          .as("_sup_bidder_advertiser_pb_LOOKUP7"),
        col("in1._sup_bidder_advertiser_pb_LOOKUP3")
          .as("_sup_bidder_advertiser_pb_LOOKUP3"),
        col("in1._sup_bidder_advertiser_pb_LOOKUP")
          .as("_sup_bidder_advertiser_pb_LOOKUP"),
        col("in1._sup_bidder_advertiser_pb_LOOKUP4")
          .as("_sup_bidder_advertiser_pb_LOOKUP4"),
        col("in1._sup_bidder_advertiser_pb_LOOKUP6")
          .as("_sup_bidder_advertiser_pb_LOOKUP6"),
        col("in1._sup_bidder_advertiser_pb_LOOKUP2")
          .as("_sup_bidder_advertiser_pb_LOOKUP2"),
        col("in1._sup_creative_media_subtype_pb_LOOKUP4")
          .as("_sup_creative_media_subtype_pb_LOOKUP4"),
        col("in1._sup_creative_media_subtype_pb_LOOKUP1")
          .as("_sup_creative_media_subtype_pb_LOOKUP1"),
        col("in1._sup_creative_media_subtype_pb_LOOKUP6")
          .as("_sup_creative_media_subtype_pb_LOOKUP6"),
        col("in1._sup_creative_media_subtype_pb_LOOKUP3")
          .as("_sup_creative_media_subtype_pb_LOOKUP3"),
        col("in1._sup_creative_media_subtype_pb_LOOKUP2")
          .as("_sup_creative_media_subtype_pb_LOOKUP2"),
        col("in1._sup_creative_media_subtype_pb_LOOKUP5")
          .as("_sup_creative_media_subtype_pb_LOOKUP5"),
        col("in1._sup_creative_media_subtype_pb_LOOKUP")
          .as("_sup_creative_media_subtype_pb_LOOKUP"),
        col("in1._sup_placement_video_attributes_pb_LOOKUP")
          .as("_sup_placement_video_attributes_pb_LOOKUP"),
        col("in0.*")
      )

}
