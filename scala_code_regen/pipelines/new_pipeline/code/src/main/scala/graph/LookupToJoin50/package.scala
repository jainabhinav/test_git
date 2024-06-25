package graph

import io.prophecy.libs._
import udfs.PipelineInitCode._
import graph.LookupToJoin50.config._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._
package object LookupToJoin50 {

  def apply(
    context: Context,
    in4:     DataFrame,
    in3:     DataFrame,
    in2:     DataFrame,
    in1:     DataFrame,
    in:      DataFrame
  ): DataFrame = {
    val df_Reformat_agg_dw_impressions_PrevExpressionj0_join0 =
      Reformat_agg_dw_impressions_PrevExpressionj0_join0(context, in)
    val df_Reformat_agg_dw_impressions_PrevExpressionj0_final0 =
      Reformat_agg_dw_impressions_PrevExpressionj0_final0(
        context,
        df_Reformat_agg_dw_impressions_PrevExpressionj0_join0
      )
    val df_Reformat_agg_dw_impressions_PrevExpressionj1_Drop_Result0 =
      Reformat_agg_dw_impressions_PrevExpressionj1_Drop_Result0(
        context,
        df_Reformat_agg_dw_impressions_PrevExpressionj0_final0
      )
    val df_Reformat_agg_dw_impressions_PrevExpressionj1_dedup1 =
      Reformat_agg_dw_impressions_PrevExpressionj1_dedup1(context, in4)
    val df_Reformat_agg_dw_impressions_PrevExpressionj1_dedup2 =
      Reformat_agg_dw_impressions_PrevExpressionj1_dedup2(context, in2)
    val df_Reformat_agg_dw_impressions_PrevExpressionj1_dedup3 =
      Reformat_agg_dw_impressions_PrevExpressionj1_dedup3(context, in3)
    val df_Reformat_agg_dw_impressions_PrevExpressionj1_dedup4 =
      Reformat_agg_dw_impressions_PrevExpressionj1_dedup4(context, in2)
    val df_Reformat_agg_dw_impressions_PrevExpressionj1 =
      Reformat_agg_dw_impressions_PrevExpressionj1(
        context,
        df_Reformat_agg_dw_impressions_PrevExpressionj1_Drop_Result0,
        df_Reformat_agg_dw_impressions_PrevExpressionj1_dedup1,
        df_Reformat_agg_dw_impressions_PrevExpressionj1_dedup2,
        df_Reformat_agg_dw_impressions_PrevExpressionj1_dedup3,
        df_Reformat_agg_dw_impressions_PrevExpressionj1_dedup4
      )
    val df_Reformat_agg_dw_impressions_PrevExpressionj2_dedup1 =
      Reformat_agg_dw_impressions_PrevExpressionj2_dedup1(context, in3)
    val df_Reformat_agg_dw_impressions_PrevExpressionj2 =
      Reformat_agg_dw_impressions_PrevExpressionj2(
        context,
        df_Reformat_agg_dw_impressions_PrevExpressionj1,
        df_Reformat_agg_dw_impressions_PrevExpressionj2_dedup1
      )
    val df_Reformat_agg_dw_impressions_PrevExpression_Drop_RowID =
      Reformat_agg_dw_impressions_PrevExpression_Drop_RowID(
        context,
        df_Reformat_agg_dw_impressions_PrevExpressionj2
      )
    df_Reformat_agg_dw_impressions_PrevExpression_Drop_RowID
  }

}
