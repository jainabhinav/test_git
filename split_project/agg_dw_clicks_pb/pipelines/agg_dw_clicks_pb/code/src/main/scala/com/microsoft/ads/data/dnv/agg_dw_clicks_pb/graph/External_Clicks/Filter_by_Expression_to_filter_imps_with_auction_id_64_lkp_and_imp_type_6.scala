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

object Filter_by_Expression_to_filter_imps_with_auction_id_64_lkp_and_imp_type_6 {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.filter(
      is_not_null(col("_lkp_log_impbus_clicks_pb_auctions_LOOKUP"))
        .and(col("imp_type") === lit(6))
        .and(col("_lkp_log_msan_map_pb_buyer_member_LOOKUP").isNull)
    )

}
