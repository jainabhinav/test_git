package com.microsoft.ads.data.dnv.agg_dw_clicks_pb.graph.External_Clicks

import io.prophecy.libs._
import com.microsoft.ads.data.dnv.agg_dw_clicks_pb.udfs.PipelineInitCode._
import com.microsoft.ads.data.dnv.agg_dw_clicks_pb.graph.External_Clicks.LookupToJoin20.config._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._
package object LookupToJoin20 {

  def apply(
    context: Context,
    in2:     DataFrame,
    in1:     DataFrame,
    in:      DataFrame
  ): DataFrame = {
    val df_External_Clicks__Filter_by_Expression_to_filter_imps_with_auction_id_64_lkp_and_imp_type_6j0_dedup1 =
      External_Clicks__Filter_by_Expression_to_filter_imps_with_auction_id_64_lkp_and_imp_type_6j0_dedup1(
        context,
        in
      )
    val df_External_Clicks__Filter_by_Expression_to_filter_imps_with_auction_id_64_lkp_and_imp_type_6j0_dedup2 =
      External_Clicks__Filter_by_Expression_to_filter_imps_with_auction_id_64_lkp_and_imp_type_6j0_dedup2(
        context,
        in1
      )
    val df_External_Clicks__Filter_by_Expression_to_filter_imps_with_auction_id_64_lkp_and_imp_type_6j0 =
      External_Clicks__Filter_by_Expression_to_filter_imps_with_auction_id_64_lkp_and_imp_type_6j0(
        context,
        in2,
        df_External_Clicks__Filter_by_Expression_to_filter_imps_with_auction_id_64_lkp_and_imp_type_6j0_dedup1,
        df_External_Clicks__Filter_by_Expression_to_filter_imps_with_auction_id_64_lkp_and_imp_type_6j0_dedup2
      )
    df_External_Clicks__Filter_by_Expression_to_filter_imps_with_auction_id_64_lkp_and_imp_type_6j0
  }

}
