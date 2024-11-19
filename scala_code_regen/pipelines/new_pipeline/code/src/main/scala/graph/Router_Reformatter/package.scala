package graph

import io.prophecy.libs._
import udfs.PipelineInitCode._
import graph.Router_Reformatter.config._
import graph.Router_Reformatter.LookupToJoin70
import graph.Router_Reformatter.LookupToJoin00
import graph.Router_Reformatter.LookupToJoin10
import graph.Router_Reformatter.LookupToJoin20
import graph.Router_Reformatter.LookupToJoin40
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._
package object Router_Reformatter {

  def apply(
    context: Context,
    in11:    DataFrame,
    in10:    DataFrame,
    in9:     DataFrame,
    in8:     DataFrame,
    in7:     DataFrame,
    in6:     DataFrame,
    in5:     DataFrame,
    in4:     DataFrame,
    in3:     DataFrame,
    in2:     DataFrame,
    in1:     DataFrame,
    in:      DataFrame
  ): Subgraph5 = {
    val df_LookupToJoin70 = LookupToJoin70.apply(
      LookupToJoin70.config
        .Context(context.spark, context.config.LookupToJoin70),
      in,
      in1,
      in2,
      in3
    )
    val (df_Reformat_TRAN_Router_Reformatter_RowDistributor_out0,
         df_Reformat_TRAN_Router_Reformatter_RowDistributor_out1,
         df_Reformat_TRAN_Router_Reformatter_RowDistributor_out2,
         df_Reformat_TRAN_Router_Reformatter_RowDistributor_out3,
         df_Reformat_TRAN_Router_Reformatter_RowDistributor_out4
    ) = Reformat_TRAN_Router_Reformatter_RowDistributor(context,
                                                        df_LookupToJoin70
    )
    val df_Reformat_TRAN_Router_Reformatter_RowDistributor__Dropout3 =
      Reformat_TRAN_Router_Reformatter_RowDistributor__Dropout3(
        context,
        df_Reformat_TRAN_Router_Reformatter_RowDistributor_out3
      )
    val df_Reformat_TRAN_Router_ReformatterReformat_3 =
      Reformat_TRAN_Router_ReformatterReformat_3(
        context,
        df_Reformat_TRAN_Router_Reformatter_RowDistributor__Dropout3
      )
    val df_Reformat_TRAN_Router_Reformatter_RowDistributor__Dropout4 =
      Reformat_TRAN_Router_Reformatter_RowDistributor__Dropout4(
        context,
        df_Reformat_TRAN_Router_Reformatter_RowDistributor_out4
      )
    val df_LookupToJoin00 = LookupToJoin00.apply(
      LookupToJoin00.config
        .Context(context.spark, context.config.LookupToJoin00),
      in8,
      in1,
      in9,
      in2,
      in3,
      in10,
      in11,
      df_Reformat_TRAN_Router_Reformatter_RowDistributor__Dropout4
    )
    val df_Reformat_TRAN_Router_ReformatterReformat_4 =
      Reformat_TRAN_Router_ReformatterReformat_4(context, df_LookupToJoin00)
    val df_Reformat_TRAN_Router_Reformatter_RowDistributor__Dropout0 =
      Reformat_TRAN_Router_Reformatter_RowDistributor__Dropout0(
        context,
        df_Reformat_TRAN_Router_Reformatter_RowDistributor_out0
      )
    val df_Reformat_TRAN_Router_Reformatter_RowDistributor__Dropout1 =
      Reformat_TRAN_Router_Reformatter_RowDistributor__Dropout1(
        context,
        df_Reformat_TRAN_Router_Reformatter_RowDistributor_out1
      )
    val df_Reformat_TRAN_Router_Reformatter_RowDistributor__Dropout2 =
      Reformat_TRAN_Router_Reformatter_RowDistributor__Dropout2(
        context,
        df_Reformat_TRAN_Router_Reformatter_RowDistributor_out2
      )
    val df_LookupToJoin10 = LookupToJoin10.apply(
      LookupToJoin10.config
        .Context(context.spark, context.config.LookupToJoin10),
      in7,
      in5,
      df_Reformat_TRAN_Router_Reformatter_RowDistributor__Dropout2
    )
    val df_Reformat_TRAN_Router_ReformatterReformat_2 =
      Reformat_TRAN_Router_ReformatterReformat_2(context, df_LookupToJoin10)
    val df_LookupToJoin20 = LookupToJoin20.apply(
      LookupToJoin20.config
        .Context(context.spark, context.config.LookupToJoin20),
      in7,
      in4,
      in5,
      df_Reformat_TRAN_Router_Reformatter_RowDistributor__Dropout1
    )
    val df_Reformat_TRAN_Router_ReformatterReformat_1 =
      Reformat_TRAN_Router_ReformatterReformat_1(context, df_LookupToJoin20)
    val df_LookupToJoin40 = LookupToJoin40.apply(
      LookupToJoin40.config
        .Context(context.spark, context.config.LookupToJoin40),
      in4,
      in5,
      in4,
      in6,
      in6,
      df_Reformat_TRAN_Router_Reformatter_RowDistributor__Dropout0
    )
    val df_Reformat_TRAN_Router_ReformatterReformat_0 =
      Reformat_TRAN_Router_ReformatterReformat_0(context, df_LookupToJoin40)
    (df_Reformat_TRAN_Router_ReformatterReformat_3,
     df_Reformat_TRAN_Router_ReformatterReformat_0,
     df_Reformat_TRAN_Router_ReformatterReformat_2,
     df_Reformat_TRAN_Router_ReformatterReformat_4,
     df_Reformat_TRAN_Router_ReformatterReformat_1
    )
  }

}
