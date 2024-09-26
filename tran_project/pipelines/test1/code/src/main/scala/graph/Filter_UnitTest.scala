package graph

import io.prophecy.libs._
import udfs.PipelineInitCode._
import udfs.UDFs._
import udfs.ColumnFunctions._
import config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Filter_UnitTest {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.filter(
      (col("auction_id_64") === lit(5432556433508205017L))
        .or(col("auction_id_64") === lit(118222924755900017L))
        .or(col("auction_id_64") === lit(335885028520017L))
        .or(col("auction_id_64") === lit(326545870850017L))
    )

}
