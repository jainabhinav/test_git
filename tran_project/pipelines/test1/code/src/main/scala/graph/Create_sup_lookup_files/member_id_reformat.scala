package graph.Create_sup_lookup_files

import io.prophecy.libs._
import udfs.PipelineInitCode._
import udfs.UDFs._
import udfs.ColumnFunctions._
import graph.Create_sup_lookup_files.config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object member_id_reformat {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.select(col("member_id").cast(IntegerType).as("member_id"),
              col("sales_tax_rate_pct"),
              col("deleted").cast(IntegerType).as("deleted")
    )

}
