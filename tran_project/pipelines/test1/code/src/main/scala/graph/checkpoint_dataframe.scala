package graph

import io.prophecy.libs._
import config.Context
import udfs.UDFs._
import udfs.ColumnFunctions._
import udfs.PipelineInitCode._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object checkpoint_dataframe {
  def apply(context: Context, in0: DataFrame): DataFrame = {
    val spark = context.spark
    val Config = context.config
    spark.sparkContext.setCheckpointDir("dbfs:/FileStore/data_engg/msbing/transaction_graph/checkpoint/dbg_checkpoint")
    val out0 = in0.checkpoint()
    out0
  }

}
