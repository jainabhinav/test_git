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

object SetCheckpointDir {
  def apply(context: Context): Unit = {
    val spark = context.spark
    val Config = context.config
    spark.sparkContext.setCheckpointDir(s"dbfs:/checkpoint${System.currentTimeMillis}")
  }

}
