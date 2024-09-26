package graph.Router_Reformatter

import io.prophecy.libs._
import udfs.PipelineInitCode._
import udfs.UDFs._
import udfs.ColumnFunctions._
import graph.Router_Reformatter.config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object RowDistributor_1 {

  def apply(
    context: Context,
    in:      DataFrame
  ): (DataFrame, DataFrame, DataFrame, DataFrame, DataFrame) =
    (in.filter(
       (col("r_is_quarantined") === lit(0)).and(col("r_is_curated") =!= 0)
     ),
     in.filter(
       (col("r_is_quarantined") === lit(0)).and(col("r_is_transacted") =!= 0)
     ),
     in.filter(
       (col("r_is_quarantined") === lit(0)).and(col("r_is_transacted") === 0)
     ),
     in.filter((col("r_is_quarantined") === lit(0)).and(col("r_is_dw") =!= 0)),
     in.filter(col("r_is_quarantined") =!= lit(0))
    )

}
