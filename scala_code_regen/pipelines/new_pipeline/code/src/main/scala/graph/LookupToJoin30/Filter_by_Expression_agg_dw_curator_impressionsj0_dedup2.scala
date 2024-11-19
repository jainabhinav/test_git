package graph.LookupToJoin30

import io.prophecy.libs._
import udfs.PipelineInitCode._
import udfs.UDFs._
import udfs.ColumnFunctions._
import graph.LookupToJoin30.config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Filter_by_Expression_agg_dw_curator_impressionsj0_dedup2 {

  def apply(context: Context, in: DataFrame): DataFrame = {
    import org.apache.spark.sql.expressions.Window
    in.withColumn("row_number",
                  row_number().over(
                    Window.partitionBy("campaign_group_id").orderBy(lit(1))
                  )
      )
      .filter(col("row_number") === lit(1))
      .drop("row_number")
  }

}
