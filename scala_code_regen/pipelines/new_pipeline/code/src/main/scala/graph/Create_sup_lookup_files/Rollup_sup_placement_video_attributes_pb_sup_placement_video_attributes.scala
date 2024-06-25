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

object Rollup_sup_placement_video_attributes_pb_sup_placement_video_attributes {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.groupBy(col("id").cast(IntegerType).as("id"))
      .agg(
        last(col("supports_skippable"))
          .cast(IntegerType)
          .as("supports_skippable"),
        last(col("max_duration_secs"))
          .cast(IntegerType)
          .as("max_duration_secs"),
        last(col("max_ad_duration_secs"))
          .cast(IntegerType)
          .as("max_ad_duration_secs"),
        last(col("maximum_number_ads"))
          .cast(IntegerType)
          .as("maximum_number_ads"),
        last(col("start_delay_secs")).cast(IntegerType).as("start_delay_secs"),
        last(col("playback_method")).cast(IntegerType).as("playback_method"),
        last(col("video_context")).cast(IntegerType).as("video_context"),
        last(col("is_mediated")).cast(IntegerType).as("is_mediated"),
        last(col("skip_offset")).cast(IntegerType).as("skip_offset")
      )

}
