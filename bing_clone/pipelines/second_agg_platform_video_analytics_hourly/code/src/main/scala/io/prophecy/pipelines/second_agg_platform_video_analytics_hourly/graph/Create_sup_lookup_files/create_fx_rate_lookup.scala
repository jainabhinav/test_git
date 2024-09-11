package io.prophecy.pipelines.second_agg_platform_video_analytics_hourly.graph.Create_sup_lookup_files

import io.prophecy.libs._
import io.prophecy.pipelines.second_agg_platform_video_analytics_hourly.graph.Create_sup_lookup_files.config.Context
import io.prophecy.pipelines.second_agg_platform_video_analytics_hourly.udfs.UDFs._
import io.prophecy.pipelines.second_agg_platform_video_analytics_hourly.udfs.ColumnFunctions._
import io.prophecy.pipelines.second_agg_platform_video_analytics_hourly.udfs.PipelineInitCode._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object create_fx_rate_lookup {

  def apply(context: Context, in0: DataFrame): Unit =
    createLookup("sup_code_fx_rate",
                 in0,
                 context.spark,
                 List("code"),
                 "fx_rate_snapshot_id",
                 "currency_id",
                 "code",
                 "rate",
                 "as_of_timestamp"
    )

}
