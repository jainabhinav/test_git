package com.microsoft.ads.data.dnv.agg_dw_clicks_pb.graph.agg_dw_clicks_initialize_lookups

import io.prophecy.libs._
import com.microsoft.ads.data.dnv.agg_dw_clicks_pb.udfs.PipelineInitCode._
import com.microsoft.ads.data.dnv.agg_dw_clicks_pb.udfs.UDFs._
import com.microsoft.ads.data.dnv.agg_dw_clicks_pb.udfs.ColumnFunctions._
import com.microsoft.ads.data.dnv.agg_dw_clicks_pb.graph.agg_dw_clicks_initialize_lookups.config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Reformat_to_select_auction_id_64_msan {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.select(col("auction_id_64").cast(LongType).as("auction_id_64"))

}