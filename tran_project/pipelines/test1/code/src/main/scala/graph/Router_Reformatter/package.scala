package graph

import io.prophecy.libs._
import udfs.PipelineInitCode._
import graph.Router_Reformatter.config._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._
package object Router_Reformatter {

  def apply(
    context: Context,
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
    val df_Reformat_TRAN_Router_Reformatter_RowDistributorj0 =
      Reformat_TRAN_Router_Reformatter_RowDistributorj0(context,
                                                        in,
                                                        in1,
                                                        in2,
                                                        in3
      )
    val df_Reformat_1 =
      Reformat_1(context, df_Reformat_TRAN_Router_Reformatter_RowDistributorj0)
    val (df_RowDistributor_1_curated,
         df_RowDistributor_1_adi_transacted_ssd,
         df_RowDistributor_1_ssd_untransacted,
         df_RowDistributor_1_adi,
         df_RowDistributor_1_quarantine
    ) = RowDistributor_1(context, df_Reformat_1)
    val df_Reformat_TRAN_Router_Reformatter_RowDistributor__Dropout0_1 =
      Reformat_TRAN_Router_Reformatter_RowDistributor__Dropout0_1(
        context,
        df_RowDistributor_1_adi_transacted_ssd
      )
    val df_Reformat_TRAN_Router_ReformatterReformat_1j0 =
      Reformat_TRAN_Router_ReformatterReformat_1j0(
        context,
        df_Reformat_TRAN_Router_Reformatter_RowDistributor__Dropout0_1,
        in5,
        in4,
        in6
      )
    val df_reformat_view_detection = reformat_view_detection(
      context,
      df_Reformat_TRAN_Router_ReformatterReformat_1j0
    )
    val df_Reformat_TRAN_Router_Reformatter_RowDistributor__Dropout0_3 =
      Reformat_TRAN_Router_Reformatter_RowDistributor__Dropout0_3(
        context,
        df_RowDistributor_1_adi
      )
    val df_Reformat_TRAN_Router_ReformatterReformat_3 =
      Reformat_TRAN_Router_ReformatterReformat_3(
        context,
        df_Reformat_TRAN_Router_Reformatter_RowDistributor__Dropout0_3
      )
    val df_Reformat_TRAN_Router_Reformatter_RowDistributor__Dropout0_2 =
      Reformat_TRAN_Router_Reformatter_RowDistributor__Dropout0_2(
        context,
        df_RowDistributor_1_ssd_untransacted
      )
    val df_Reformat_TRAN_Router_ReformatterReformat_2j0 =
      Reformat_TRAN_Router_ReformatterReformat_2j0(
        context,
        df_Reformat_TRAN_Router_Reformatter_RowDistributor__Dropout0_2,
        in5,
        in6
      )
    val df_Reformat_TRAN_Router_Reformatter_RowDistributor__Dropout0_4 =
      Reformat_TRAN_Router_Reformatter_RowDistributor__Dropout0_4(
        context,
        df_RowDistributor_1_quarantine
      )
    val df_Reformat_TRAN_Router_ReformatterReformat_4j0 =
      Reformat_TRAN_Router_ReformatterReformat_4j0(
        context,
        df_Reformat_TRAN_Router_Reformatter_RowDistributor__Dropout0_4,
        in7,
        in8,
        in1,
        in2,
        in9,
        in3,
        in10
      )
    val df_Reformat_TRAN_Router_ReformatterReformat_4 =
      Reformat_TRAN_Router_ReformatterReformat_4(
        context,
        df_Reformat_TRAN_Router_ReformatterReformat_4j0
      )
    val df_Reformat_TRAN_Router_Reformatter_RowDistributor__Dropout0 =
      Reformat_TRAN_Router_Reformatter_RowDistributor__Dropout0(
        context,
        df_RowDistributor_1_curated
      )
    val df_Reformat_TRAN_Router_ReformatterReformat_2 =
      Reformat_TRAN_Router_ReformatterReformat_2(
        context,
        df_Reformat_TRAN_Router_ReformatterReformat_2j0
      )
    val df_Reformat_TRAN_Router_ReformatterReformat_1 =
      Reformat_TRAN_Router_ReformatterReformat_1(
        context,
        df_Reformat_TRAN_Router_ReformatterReformat_1j0
      )
    val df_Reformat_TRAN_Router_ReformatterReformat_0j0 =
      Reformat_TRAN_Router_ReformatterReformat_0j0(
        context,
        df_Reformat_TRAN_Router_Reformatter_RowDistributor__Dropout0,
        in4,
        in5,
        in4
      )
    val df_Reformat_TRAN_Router_ReformatterReformat_0 =
      Reformat_TRAN_Router_ReformatterReformat_0(
        context,
        df_Reformat_TRAN_Router_ReformatterReformat_0j0
      )
    (df_Reformat_TRAN_Router_ReformatterReformat_3,
     df_Reformat_TRAN_Router_ReformatterReformat_0,
     df_Reformat_TRAN_Router_ReformatterReformat_2,
     df_Reformat_TRAN_Router_ReformatterReformat_4,
     df_Reformat_TRAN_Router_ReformatterReformat_1
    )
  }

}
