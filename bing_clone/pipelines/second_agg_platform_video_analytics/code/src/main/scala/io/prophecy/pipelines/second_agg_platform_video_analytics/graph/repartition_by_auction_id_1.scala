package io.prophecy.pipelines.second_agg_platform_video_analytics.graph

import io.prophecy.libs._
import io.prophecy.pipelines.second_agg_platform_video_analytics.config.Context
import io.prophecy.pipelines.second_agg_platform_video_analytics.udfs.UDFs._
import io.prophecy.pipelines.second_agg_platform_video_analytics.udfs.ColumnFunctions._
import io.prophecy.pipelines.second_agg_platform_video_analytics.udfs.PipelineInitCode._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object repartition_by_auction_id_1 {
  def apply(context: Context, in0: DataFrame): DataFrame = {
    val spark = context.spark
    val Config = context.config
    println("#####Step name: repartition by auction id#####")
    println("step start time: " + Instant.now().atZone(ZoneId.of("America/Chicago"))) 
    
    import org.apache.spark.storage.StorageLevel
    val rep_count = 12000
    val out0 = in0.repartition(rep_count, col("auction_id_64")).persist(StorageLevel.DISK_ONLY)
    
    println("#####Step name: repartition by auction id#####")
    println("step end time: " + Instant.now().atZone(ZoneId.of("America/Chicago"))) 
    out0
  }

}
