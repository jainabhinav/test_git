package graph.LookupToJoin50

import io.prophecy.libs._
import udfs.PipelineInitCode._
import udfs.UDFs._
import udfs.ColumnFunctions._
import graph.LookupToJoin50.config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Reformat_agg_dw_impressions_PrevExpressionj2 {

  def apply(context: Context, in0: DataFrame, in1: DataFrame): DataFrame =
    in0
      .as("in0")
      .join(
        in1.as("in1").hint("broadcast"),
        when(
          f_transaction_event(
            col("in0.log_impbus_impressions.buyer_transaction_def"),
            col("in0.log_impbus_preempt.buyer_transaction_def")
          ) === lit(1),
          when(
            is_not_null(col("in0.log_dw_bid_deal.data_costs")),
            coalesce(col("in0._sup_common_deal_LOOKUP").getField("member_id"),
                     lit(0)
            )
          ).otherwise(lit(null))
        ).otherwise(lit(null)) === col("in1.member_id"),
        "left_outer"
      )
      .select(
        when(
          is_not_null(col("in1.member_id")),
          struct(col("in1.member_id").as("member_id"),
                 col("in1.sales_tax_rate_pct").as("sales_tax_rate_pct"),
                 col("in1.deleted").as("deleted")
          )
        ).as("_sup_bidder_member_sales_tax_rate_LOOKUP"),
        col("in0.*")
      )

}
