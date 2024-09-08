package io.prophecy.pipelines.third_agg_platform_video_analytics.graph

import io.prophecy.libs._
import io.prophecy.pipelines.third_agg_platform_video_analytics.config.Context
import io.prophecy.pipelines.third_agg_platform_video_analytics.udfs.UDFs._
import io.prophecy.pipelines.third_agg_platform_video_analytics.udfs.ColumnFunctions._
import io.prophecy.pipelines.third_agg_platform_video_analytics.udfs.PipelineInitCode._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object complex_join_with_lookups {
  def apply(context: Context, in0: DataFrame, in1: DataFrame): DataFrame = {
    val spark = context.spark
    val Config = context.config
    // Helper function to define the join condition based on advertiser and member IDs, with proper handling for lit(1) and lit(0)
      def joinCondition(prefix: String, flag: String, alias: String, isBuySide: Boolean): Column = {
        val sideCondition = if (isBuySide) {
          when(col(s"in0.$flag") === lit(1), col(s"in0.${prefix}_buyer_member_id"))
            .otherwise(col(s"in0.${prefix}_seller_member_id"))
        } else {
          when(col(s"in0.$flag") === lit(0), col(s"in0.${prefix}_buyer_member_id"))
            .otherwise(col(s"in0.${prefix}_seller_member_id"))
        }
    
        (col(s"in0.${prefix}_advertiser_id") === col(s"$alias.id"))
          .and(sideCondition === col(s"$alias.member_id"))
      }
    
      // Helper function to create the struct column after join
      def createLookupStruct(alias: String): Column = {
        when(
          is_not_null(col(s"$alias.id")).and(is_not_null(col(s"$alias.member_id"))),
          struct(
            col(s"$alias.id").as("id"),
            col(s"$alias.member_id").as("member_id"),
            col(s"$alias.advertiser_default_currency").as("advertiser_default_currency"),
            col(s"$alias.is_running_political_ads").as("is_running_political_ads"),
            col(s"$alias.name").as("name")
          )
        )
      }
    
    val new_in1 = in1.cache()
    new_in1.count()
    
      // Perform joins with the same DataFrame using different join conditions (handling both lit(1) and lit(0) cases)
      val joinedDF = in0.as("in0")
        .join(new_in1.as("in1"), joinCondition("agg_dw_pixels", "f_is_buy_side_imp_agg_dw_pixels_imp_type", "in1", isBuySide = true), "left_outer")
        .join(new_in1.as("in2"), joinCondition("agg_dw_video_events", "f_is_buy_side_imp_imp_type_request_imp_type", "in2", isBuySide = true), "left_outer")
        .join(new_in1.as("in3"), joinCondition("agg_platform_video_impressions", "f_is_buy_side_imp_agg_platform_video_impressions_imp_type", "in3", isBuySide = true), "left_outer")
        .join(new_in1.as("in4"), joinCondition("agg_dw_clicks", "f_is_buy_side_imp_agg_dw_clicks_imp_type", "in4", isBuySide = false), "left_outer")
        .join(new_in1.as("in5"), joinCondition("agg_platform_video_impressions", "f_is_buy_side_imp_agg_platform_video_impressions_imp_type", "in5", isBuySide = false), "left_outer")
        .join(new_in1.as("in6"), joinCondition("agg_dw_video_events", "f_is_buy_side_imp_imp_type_request_imp_type", "in6", isBuySide = false), "left_outer")
        .join(new_in1.as("in7"), joinCondition("agg_dw_clicks", "f_is_buy_side_imp_agg_dw_clicks_imp_type", "in7", isBuySide = true), "left_outer")
        .join(new_in1.as("in8"), joinCondition("agg_dw_pixels", "f_is_buy_side_imp_agg_dw_pixels_imp_type", "in8", isBuySide = false), "left_outer")
    
      // Add the lookup columns based on the joined DataFrames
      val out0 = joinedDF
        .withColumn("_sup_bidder_advertiser_pb_LOOKUP1", createLookupStruct("in1"))
        .withColumn("_sup_bidder_advertiser_pb_LOOKUP5", createLookupStruct("in2"))
        .withColumn("_sup_bidder_advertiser_pb_LOOKUP7", createLookupStruct("in3"))
        .withColumn("_sup_bidder_advertiser_pb_LOOKUP3", createLookupStruct("in4"))
        .withColumn("_sup_bidder_advertiser_pb_LOOKUP", createLookupStruct("in5"))
        .withColumn("_sup_bidder_advertiser_pb_LOOKUP4", createLookupStruct("in6"))
        .withColumn("_sup_bidder_advertiser_pb_LOOKUP6", createLookupStruct("in7"))
        .withColumn("_sup_bidder_advertiser_pb_LOOKUP2", createLookupStruct("in8"))
        .select(
        col("in0.*"),  // Select all columns from in0
        col("_sup_bidder_advertiser_pb_LOOKUP1"),  // Include the newly generated columns
        col("_sup_bidder_advertiser_pb_LOOKUP5"),
        col("_sup_bidder_advertiser_pb_LOOKUP7"),
        col("_sup_bidder_advertiser_pb_LOOKUP3"),
        col("_sup_bidder_advertiser_pb_LOOKUP"),
        col("_sup_bidder_advertiser_pb_LOOKUP4"),
        col("_sup_bidder_advertiser_pb_LOOKUP6"),
        col("_sup_bidder_advertiser_pb_LOOKUP2"),
        col("_sup_placement_video_attributes_pb_LOOKUP")
      )
     
    out0
  }

}
