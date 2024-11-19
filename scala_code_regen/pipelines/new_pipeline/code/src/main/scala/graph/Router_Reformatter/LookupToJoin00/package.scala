package graph.Router_Reformatter

import io.prophecy.libs._
import udfs.PipelineInitCode._
import graph.Router_Reformatter.LookupToJoin00.config._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._
package object LookupToJoin00 {

  def apply(
    context: Context,
    in7:     DataFrame,
    in6:     DataFrame,
    in5:     DataFrame,
    in4:     DataFrame,
    in3:     DataFrame,
    in2:     DataFrame,
    in1:     DataFrame,
    in:      DataFrame
  ): DataFrame = {
    val df_Router_Reformatter__Reformat_TRAN_Router_ReformatterReformat_4j0_dedup4 =
      Router_Reformatter__Reformat_TRAN_Router_ReformatterReformat_4j0_dedup4(
        context,
        in4
      )
    val df_Router_Reformatter__Reformat_TRAN_Router_ReformatterReformat_4j0_dedup7 =
      Router_Reformatter__Reformat_TRAN_Router_ReformatterReformat_4j0_dedup7(
        context,
        in7
      )
    val df_Router_Reformatter__Reformat_TRAN_Router_ReformatterReformat_4j0_dedup5 =
      Router_Reformatter__Reformat_TRAN_Router_ReformatterReformat_4j0_dedup5(
        context,
        in5
      )
    val df_Router_Reformatter__Reformat_TRAN_Router_ReformatterReformat_4j0_dedup1 =
      Router_Reformatter__Reformat_TRAN_Router_ReformatterReformat_4j0_dedup1(
        context,
        in1
      )
    val df_Router_Reformatter__Reformat_TRAN_Router_ReformatterReformat_4j0_dedup6 =
      Router_Reformatter__Reformat_TRAN_Router_ReformatterReformat_4j0_dedup6(
        context,
        in6
      )
    val df_Router_Reformatter__Reformat_TRAN_Router_ReformatterReformat_4j0_dedup2 =
      Router_Reformatter__Reformat_TRAN_Router_ReformatterReformat_4j0_dedup2(
        context,
        in2
      )
    val df_Router_Reformatter__Reformat_TRAN_Router_ReformatterReformat_4j0_dedup3 =
      Router_Reformatter__Reformat_TRAN_Router_ReformatterReformat_4j0_dedup3(
        context,
        in3
      )
    val df_Router_Reformatter__Reformat_TRAN_Router_ReformatterReformat_4j0 =
      Router_Reformatter__Reformat_TRAN_Router_ReformatterReformat_4j0(
        context,
        in,
        df_Router_Reformatter__Reformat_TRAN_Router_ReformatterReformat_4j0_dedup1,
        df_Router_Reformatter__Reformat_TRAN_Router_ReformatterReformat_4j0_dedup2,
        df_Router_Reformatter__Reformat_TRAN_Router_ReformatterReformat_4j0_dedup3,
        df_Router_Reformatter__Reformat_TRAN_Router_ReformatterReformat_4j0_dedup4,
        df_Router_Reformatter__Reformat_TRAN_Router_ReformatterReformat_4j0_dedup5,
        df_Router_Reformatter__Reformat_TRAN_Router_ReformatterReformat_4j0_dedup6,
        df_Router_Reformatter__Reformat_TRAN_Router_ReformatterReformat_4j0_dedup7
      )
    df_Router_Reformatter__Reformat_TRAN_Router_ReformatterReformat_4j0
  }

}
