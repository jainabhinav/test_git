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
    in3:     DataFrame,
    in2:     DataFrame,
    in1:     DataFrame,
    in:      DataFrame
  ): DataFrame = {
    val df_Reformat_agg_dw_impressions_PrevExpressionj0_RowID =
      Reformat_agg_dw_impressions_PrevExpressionj0_RowID(context, in3)
    val df_Reformat_agg_dw_impressions_PrevExpressionj0 =
      Reformat_agg_dw_impressions_PrevExpressionj0(
        context,
        df_Reformat_agg_dw_impressions_PrevExpressionj0_RowID,
        in1,
        in1,
        in2
      )
    val df_Reformat_agg_dw_impressions_PrevExpressionj1_join0 =
      Reformat_agg_dw_impressions_PrevExpressionj1_join0(
        context,
        df_Reformat_agg_dw_impressions_PrevExpressionj0,
        in
      )
    val df_Reformat_agg_dw_impressions_PrevExpressionj1_final0 =
      Reformat_agg_dw_impressions_PrevExpressionj1_final0(
        context,
        df_Reformat_agg_dw_impressions_PrevExpressionj1_join0
      )
    val df_Reformat_agg_dw_impressions_PrevExpression_Drop_RowID_Drop_Result0 =
      Reformat_agg_dw_impressions_PrevExpression_Drop_RowID_Drop_Result0(
        context,
        df_Reformat_agg_dw_impressions_PrevExpressionj1_final0
      )
    val df_Reformat_agg_dw_impressions_PrevExpression_Drop_RowID =
      Reformat_agg_dw_impressions_PrevExpression_Drop_RowID(
        context,
        df_Reformat_agg_dw_impressions_PrevExpression_Drop_RowID_Drop_Result0
      )
    df_Reformat_agg_dw_impressions_PrevExpression_Drop_RowID
  }

}
