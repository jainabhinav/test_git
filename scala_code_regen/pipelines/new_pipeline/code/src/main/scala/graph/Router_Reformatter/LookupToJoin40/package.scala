package graph.Router_Reformatter

import io.prophecy.libs._
import udfs.PipelineInitCode._
import graph.Router_Reformatter.LookupToJoin40.config._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._
package object LookupToJoin40 {

  def apply(
    context: Context,
    in5:     DataFrame,
    in4:     DataFrame,
    in3:     DataFrame,
    in2:     DataFrame,
    in1:     DataFrame,
    in:      DataFrame
  ): DataFrame = {
    val df_Router_Reformatter__Reformat_TRAN_Router_ReformatterReformat_0j0_dedup3 =
      Router_Reformatter__Reformat_TRAN_Router_ReformatterReformat_0j0_dedup3(
        context,
        in5
      )
    val df_Router_Reformatter__Reformat_TRAN_Router_ReformatterReformat_0j0_dedup1 =
      Router_Reformatter__Reformat_TRAN_Router_ReformatterReformat_0j0_dedup1(
        context,
        in3
      )
    val df_Router_Reformatter__Reformat_TRAN_Router_ReformatterReformat_0j0_dedup2 =
      Router_Reformatter__Reformat_TRAN_Router_ReformatterReformat_0j0_dedup2(
        context,
        in4
      )
    val df_Router_Reformatter__Reformat_TRAN_Router_ReformatterReformat_0j0 =
      Router_Reformatter__Reformat_TRAN_Router_ReformatterReformat_0j0(
        context,
        in,
        df_Router_Reformatter__Reformat_TRAN_Router_ReformatterReformat_0j0_dedup1,
        df_Router_Reformatter__Reformat_TRAN_Router_ReformatterReformat_0j0_dedup2,
        df_Router_Reformatter__Reformat_TRAN_Router_ReformatterReformat_0j0_dedup3
      )
    val df_Router_Reformatter__Reformat_TRAN_Router_ReformatterReformat_0j1_dedup2 =
      Router_Reformatter__Reformat_TRAN_Router_ReformatterReformat_0j1_dedup2(
        context,
        in2
      )
    val df_Router_Reformatter__Reformat_TRAN_Router_ReformatterReformat_0j1_dedup1 =
      Router_Reformatter__Reformat_TRAN_Router_ReformatterReformat_0j1_dedup1(
        context,
        in1
      )
    val df_Router_Reformatter__Reformat_TRAN_Router_ReformatterReformat_0j1 =
      Router_Reformatter__Reformat_TRAN_Router_ReformatterReformat_0j1(
        context,
        df_Router_Reformatter__Reformat_TRAN_Router_ReformatterReformat_0j0,
        df_Router_Reformatter__Reformat_TRAN_Router_ReformatterReformat_0j1_dedup1,
        df_Router_Reformatter__Reformat_TRAN_Router_ReformatterReformat_0j1_dedup2
      )
    df_Router_Reformatter__Reformat_TRAN_Router_ReformatterReformat_0j1
  }

}
