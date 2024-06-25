package graph

import io.prophecy.libs._
import udfs.PipelineInitCode._
import graph.LookupToJoin60.config._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._
package object LookupToJoin60 {

  def apply(
    context: Context,
    in3:     DataFrame,
    in2:     DataFrame,
    in1:     DataFrame,
    in:      DataFrame
  ): DataFrame = {
    val df_Filter_by_Expression_agg_dw_impressionsj0_dedup1 =
      Filter_by_Expression_agg_dw_impressionsj0_dedup1(context, in3)
    val df_Filter_by_Expression_agg_dw_impressionsj0_dedup2 =
      Filter_by_Expression_agg_dw_impressionsj0_dedup2(context, in2)
    val df_Filter_by_Expression_agg_dw_impressionsj0_dedup3 =
      Filter_by_Expression_agg_dw_impressionsj0_dedup3(context, in1)
    val df_Filter_by_Expression_agg_dw_impressionsj0 =
      Filter_by_Expression_agg_dw_impressionsj0(
        context,
        in,
        df_Filter_by_Expression_agg_dw_impressionsj0_dedup1,
        df_Filter_by_Expression_agg_dw_impressionsj0_dedup2,
        df_Filter_by_Expression_agg_dw_impressionsj0_dedup3
      )
    df_Filter_by_Expression_agg_dw_impressionsj0
  }

}
