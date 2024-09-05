package com.microsoft.ads.data.dnv.agg_dw_clicks_pb.graph.External_Clicks

import io.prophecy.libs._
import com.microsoft.ads.data.dnv.agg_dw_clicks_pb.udfs.PipelineInitCode._
import com.microsoft.ads.data.dnv.agg_dw_clicks_pb.udfs.UDFs._
import com.microsoft.ads.data.dnv.agg_dw_clicks_pb.udfs.ColumnFunctions._
import com.microsoft.ads.data.dnv.agg_dw_clicks_pb.graph.External_Clicks.config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Rollup_1_Reformat {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.select(
      col("date_time").cast(LongType).as("date_time"),
      col("auction_id_64").cast(LongType).as("auction_id_64"),
      col("user_id_64").cast(LongType).as("user_id_64"),
      col("referrer"),
      col("user_ip"),
      col("truncate_ip").cast(IntegerType).as("truncate_ip"),
      col("anonymized_user_info"),
      col("bidder_id").cast(IntegerType).as("bidder_id"),
      col("auction_timestamp").cast(LongType).as("auction_timestamp"),
      col("is_deferred")
    )

}
