package graph.Router_Reformatter

import io.prophecy.libs._
import udfs.PipelineInitCode._
import graph.Router_Reformatter.LookupToJoin20.config._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._
package object LookupToJoin20 {

  def apply(
    context: Context,
    in3:     DataFrame,
    in2:     DataFrame,
    in1:     DataFrame,
    in:      DataFrame
  ): DataFrame = {
    val df_Router_Reformatter__Reformat_TRAN_Router_ReformatterReformat_1j0_dedup1 =
      Router_Reformatter__Reformat_TRAN_Router_ReformatterReformat_1j0_dedup1(
        context,
        in1
      )
    val df_Router_Reformatter__Reformat_TRAN_Router_ReformatterReformat_1j0_dedup2 =
      Router_Reformatter__Reformat_TRAN_Router_ReformatterReformat_1j0_dedup2(
        context,
        in2
      )
    val df_Router_Reformatter__Reformat_TRAN_Router_ReformatterReformat_1j0_dedup3 =
      Router_Reformatter__Reformat_TRAN_Router_ReformatterReformat_1j0_dedup3(
        context,
        in3
      )
    val df_Router_Reformatter__Reformat_TRAN_Router_ReformatterReformat_1j0 =
      Router_Reformatter__Reformat_TRAN_Router_ReformatterReformat_1j0(
        context,
        in,
        df_Router_Reformatter__Reformat_TRAN_Router_ReformatterReformat_1j0_dedup1,
        df_Router_Reformatter__Reformat_TRAN_Router_ReformatterReformat_1j0_dedup2,
        df_Router_Reformatter__Reformat_TRAN_Router_ReformatterReformat_1j0_dedup3
      )
    df_Router_Reformatter__Reformat_TRAN_Router_ReformatterReformat_1j0
  }

}
