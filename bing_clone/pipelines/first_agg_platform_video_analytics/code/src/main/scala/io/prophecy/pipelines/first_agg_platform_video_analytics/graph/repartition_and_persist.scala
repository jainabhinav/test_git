package io.prophecy.pipelines.first_agg_platform_video_analytics.graph

import io.prophecy.libs._
import io.prophecy.pipelines.first_agg_platform_video_analytics.config.Context
import io.prophecy.pipelines.first_agg_platform_video_analytics.udfs.UDFs._
import io.prophecy.pipelines.first_agg_platform_video_analytics.udfs.ColumnFunctions._
import io.prophecy.pipelines.first_agg_platform_video_analytics.udfs.PipelineInitCode._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object repartition_and_persist {
  def apply(context: Context, in0: DataFrame): DataFrame = {
    val spark = context.spark
    val Config = context.config
    import org.apache.spark.storage.StorageLevel
    
    // Cache in0 to avoid recomputation
    // val rep_count = spark.conf.get("spark.sql.shuffle.partitions").toInt
    val out0 = in0.persist(StorageLevel.DISK_ONLY)
    out0.foreachPartition(_ => ())
    out0
  }

}
