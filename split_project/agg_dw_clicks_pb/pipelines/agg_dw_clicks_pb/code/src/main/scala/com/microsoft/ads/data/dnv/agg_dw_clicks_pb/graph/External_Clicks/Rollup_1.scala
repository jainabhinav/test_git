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

object Rollup_1 {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.groupBy(col("auction_id_64").cast(LongType).as("auction_id_64"))
      .agg(
        min(col("date_time").cast(LongType)).as("date_time"),
        when(min(col("user_id_64")) === lit(-1), lit(-1L))
          .otherwise(max(col("user_id_64")))
          .as("user_id_64")
          .as("user_id_64"),
        last(col("referrer")).as("referrer"),
        last(col("user_ip")).as("user_ip"),
        last(col("truncate_ip")).cast(IntegerType).as("truncate_ip"),
        first(col("anonymized_user_info")).as("anonymized_user_info"),
        last(col("bidder_id")).cast(IntegerType).as("bidder_id"),
        last(col("auction_timestamp")).cast(LongType).as("auction_timestamp"),
        last(col("is_deferred")).cast(BooleanType).as("is_deferred")
      )

}
