package graph.Router_Reformatter

import io.prophecy.libs._
import udfs.PipelineInitCode._
import graph.Router_Reformatter.LookupToJoin70.config._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._
package object LookupToJoin70 {

  def apply(
    context: Context,
    in3:     DataFrame,
    in2:     DataFrame,
    in1:     DataFrame,
    in:      DataFrame
  ): DataFrame = {
    val df_Router_Reformatter__Reformat_TRAN_Router_Reformatter_RowDistributorj0_dedup1 =
      Router_Reformatter__Reformat_TRAN_Router_Reformatter_RowDistributorj0_dedup1(
        context,
        in
      )
    val df_Router_Reformatter__Reformat_TRAN_Router_Reformatter_RowDistributorj0_dedup2 =
      Router_Reformatter__Reformat_TRAN_Router_Reformatter_RowDistributorj0_dedup2(
        context,
        in1
      )
    val df_Router_Reformatter__Reformat_TRAN_Router_Reformatter_RowDistributorj0_dedup3 =
      Router_Reformatter__Reformat_TRAN_Router_Reformatter_RowDistributorj0_dedup3(
        context,
        in2
      )
    val df_Router_Reformatter__Reformat_TRAN_Router_Reformatter_RowDistributorj0 =
      Router_Reformatter__Reformat_TRAN_Router_Reformatter_RowDistributorj0(
        context,
        in3,
        df_Router_Reformatter__Reformat_TRAN_Router_Reformatter_RowDistributorj0_dedup1,
        df_Router_Reformatter__Reformat_TRAN_Router_Reformatter_RowDistributorj0_dedup2,
        df_Router_Reformatter__Reformat_TRAN_Router_Reformatter_RowDistributorj0_dedup3
      )
    df_Router_Reformatter__Reformat_TRAN_Router_Reformatter_RowDistributorj0
  }

}
