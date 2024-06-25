package graph.Router_Reformatter.LookupToJoin40

import io.prophecy.libs._
import udfs.PipelineInitCode._
import udfs.UDFs._
import udfs.ColumnFunctions._
import graph.Router_Reformatter.LookupToJoin40.config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Router_Reformatter__Reformat_TRAN_Router_ReformatterReformat_0j1 {

  def apply(
    context: Context,
    in0:     DataFrame,
    in1:     DataFrame,
    in2:     DataFrame
  ): DataFrame =
    in0
      .as("in0")
      .join(in1.as("in1").hint("broadcast"),
            coalesce(col("in0._sup_common_deal_LOOKUP1").getField("member_id"),
                     lit(0)
            ) === col("in1.member_id"),
            "left_outer"
      )
      .join(
        in2.as("in2").hint("broadcast"),
        when(
          coalesce(
            col("in0.log_impbus_preempt.curated_deal_id").cast(IntegerType),
            lit(0)
          ) =!= lit(0),
          coalesce(col("in0._sup_common_deal_LOOKUP").getField("member_id"),
                   lit(0)
          )
        ).otherwise(lit(0)).cast(IntegerType) === col("in2.member_id"),
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
        when(
          is_not_null(col("in2.member_id")),
          struct(col("in2.member_id").as("member_id"),
                 col("in2.sales_tax_rate_pct").as("sales_tax_rate_pct"),
                 col("in2.deleted").as("deleted")
          )
        ).as("_sup_bidder_member_sales_tax_rate_LOOKUP1"),
        col("in0.*")
      )

}
