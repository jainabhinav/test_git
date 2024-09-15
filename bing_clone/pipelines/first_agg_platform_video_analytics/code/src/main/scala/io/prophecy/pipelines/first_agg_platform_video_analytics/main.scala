package io.prophecy.pipelines.first_agg_platform_video_analytics

import io.prophecy.libs._
import io.prophecy.pipelines.first_agg_platform_video_analytics.config._
import io.prophecy.pipelines.first_agg_platform_video_analytics.udfs.UDFs._
import io.prophecy.pipelines.first_agg_platform_video_analytics.udfs.ColumnFunctions._
import io.prophecy.pipelines.first_agg_platform_video_analytics.udfs.PipelineInitCode._
import io.prophecy.pipelines.first_agg_platform_video_analytics.graph._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Main {

  def apply(context: Context): Unit = {
    Script_1(context)
    val df_Main_Graph = Main_Graph
      .apply(
        Main_Graph.config.Context(context.spark, context.config.Main_Graph)
      )
      .cache()
    val df_Filter_1            = Filter_1(context,            df_Main_Graph)
    val df_select_auction_data = select_auction_data(context, df_Filter_1)
    val df_reformat_auction_data =
      reformat_auction_data(context, df_select_auction_data).cache()
    temp_output2(context,            df_reformat_auction_data)
    temp_output1(context,            df_Filter_1)
  }

  def main(args: Array[String]): Unit = {
    val config = ConfigurationFactoryImpl.getConfig(args)
    val spark: SparkSession = SparkSession
      .builder()
      .appName("Prophecy Pipeline")
      .config("spark.default.parallelism",             "4")
      .config("spark.sql.legacy.allowUntypedScalaUDF", "true")
      .enableHiveSupport()
      .getOrCreate()
    val context = Context(spark, config)
    spark.conf.set("prophecy.metadata.pipeline.uri",
                   "pipelines/first_agg_platform_video_analytics"
    )
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold",         "710485760")
    spark.conf.set("spark.sql.adaptive.enabled",                   "true")
    spark.conf.set("park.sql.adaptive.coalescePartitions.enabled", "true")
    spark.conf
      .set("spark.sql.adaptive.coalescePartitions.minPartitionSize", "512MB")
    spark.conf.set("spark.sql.adaptive.skewJoin.enabled",            "true")
    spark.conf.set("spark.sql.adaptive.join.enabled",                "true")
    spark.conf.set(
      "spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes",
      "512MB"
    )
    registerUDFs(spark)
    MetricsCollector.instrument(spark,
                                "pipelines/first_agg_platform_video_analytics"
    ) {
      apply(context)
    }
  }

}
