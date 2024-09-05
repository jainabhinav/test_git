package com.microsoft.ads.data.dnv.agg_dw_clicks_pb.graph

import io.prophecy.libs._
import com.microsoft.ads.data.dnv.agg_dw_clicks_pb.udfs.PipelineInitCode._
import com.microsoft.ads.data.dnv.agg_dw_clicks_pb.udfs.UDFs._
import com.microsoft.ads.data.dnv.agg_dw_clicks_pb.udfs.ColumnFunctions._
import com.microsoft.ads.data.dnv.agg_dw_clicks_pb.config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Join_aggDwClicksPb_AddLookupResultsj0 {

  def apply(context: Context, in0: DataFrame, in1: DataFrame): DataFrame =
    in0
      .as("in0")
      .join(in1.as("in1"),
            col("in0.ll1_buyer_member_id") === col("in1.member_id"),
            "left_outer"
      )
      .select(
        when(
          is_not_null(col("in1.member_id")),
          struct(col("in1.member_id").as("member_id"),
                 col("in1.sales_tax_rate_pct").as("sales_tax_rate_pct"),
                 col("in1.deleted").as("deleted")
          )
        ).as("_lkp_sales_tax_rate_LOOKUP"),
        col("in0.*")
      )

}
