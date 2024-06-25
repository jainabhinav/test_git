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

object Reformat_agg_dw_impressions_Checkpoint {
  def apply(context: Context, in: DataFrame): DataFrame = {
    val spark = context.spark
    val Config = context.config
    
      val out = context.config.CHECKPOINT_STRATEGY match {
        case "checkpoint" => in.checkpoint
        case "cache"      => in.cache()
        case "persist"    => in.persist()
        case "break"      => context.spark.createDataFrame(in.rdd, in.schema)
        case _            => in
      }
    out
  }

}
