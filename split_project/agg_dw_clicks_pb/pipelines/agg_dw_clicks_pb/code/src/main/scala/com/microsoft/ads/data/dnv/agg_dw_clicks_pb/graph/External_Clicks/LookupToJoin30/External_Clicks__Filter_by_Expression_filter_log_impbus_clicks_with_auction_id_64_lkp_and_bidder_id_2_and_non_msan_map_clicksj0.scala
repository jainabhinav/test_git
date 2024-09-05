package com.microsoft.ads.data.dnv.agg_dw_clicks_pb.graph.External_Clicks.LookupToJoin30

import io.prophecy.libs._
import com.microsoft.ads.data.dnv.agg_dw_clicks_pb.udfs.PipelineInitCode._
import com.microsoft.ads.data.dnv.agg_dw_clicks_pb.udfs.UDFs._
import com.microsoft.ads.data.dnv.agg_dw_clicks_pb.udfs.ColumnFunctions._
import com.microsoft.ads.data.dnv.agg_dw_clicks_pb.graph.External_Clicks.LookupToJoin30.config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object External_Clicks__Filter_by_Expression_filter_log_impbus_clicks_with_auction_id_64_lkp_and_bidder_id_2_and_non_msan_map_clicksj0 {

  def apply(
    context: Context,
    in0:     DataFrame,
    in1:     DataFrame,
    in2:     DataFrame
  ): DataFrame =
    in0
      .as("in0")
      .join(in1.as("in1"),
            col("in0.auction_id_64") === col("in1.auction_id_64"),
            "left_outer"
      )
      .join(in2.as("in2"),
            col("in0.auction_id_64") === col("in2.auction_id_64"),
            "left_outer"
      )
      .select(
        when(is_not_null(col("in1.auction_id_64")),
             struct(col("in1.auction_id_64").as("auction_id_64"))
        ).as("_lkp_log_msan_map_pb_click_auctions_LOOKUP"),
        when(is_not_null(col("in2.auction_id_64")),
             struct(col("in2.auction_id_64").as("auction_id_64"))
        ).as("_lkp_log_impbus_clicks_pb_auctions_LOOKUP"),
        col("in0.*")
      )

}
