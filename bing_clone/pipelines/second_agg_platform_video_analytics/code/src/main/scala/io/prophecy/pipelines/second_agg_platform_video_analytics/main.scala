package io.prophecy.pipelines.second_agg_platform_video_analytics

import io.prophecy.libs._
import io.prophecy.pipelines.second_agg_platform_video_analytics.config._
import io.prophecy.pipelines.second_agg_platform_video_analytics.udfs.UDFs._
import io.prophecy.pipelines.second_agg_platform_video_analytics.udfs.ColumnFunctions._
import io.prophecy.pipelines.second_agg_platform_video_analytics.udfs.PipelineInitCode._
import io.prophecy.pipelines.second_agg_platform_video_analytics.graph._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Main {

  def apply(context: Context): Unit = {
    val df_temp_output1 = temp_output1(context)
    val df_repartition_by_auction_id =
      repartition_by_auction_id(context, df_temp_output1)
    val df_select_auction_data =
      select_auction_data(context, df_repartition_by_auction_id)
    val df_reformat_auction_data =
      reformat_auction_data(context, df_select_auction_data).cache()
    val (df_Create_sup_lookup_files_out2,
         df_Create_sup_lookup_files_out1,
         df_Create_sup_lookup_files_out
    ) = Create_sup_lookup_files.apply(
      Create_sup_lookup_files.config
        .Context(context.spark, context.config.Create_sup_lookup_files)
    )
    val df_repartition_by_id =
      repartition_by_id(context, df_Create_sup_lookup_files_out2)
    val df_left_outer_join_video_attributes =
      left_outer_join_video_attributes(context,
                                       df_reformat_auction_data,
                                       df_repartition_by_id
      ).cache()
    val df_join_and_lookup_creatives = join_and_lookup_creatives(
      context,
      df_left_outer_join_video_attributes,
      df_Create_sup_lookup_files_out1
    )
    val df_complex_join_with_lookups = complex_join_with_lookups(
      context,
      df_join_and_lookup_creatives,
      df_Create_sup_lookup_files_out
    )
    Script_1(context)
    val df_join_auction_data = join_auction_data(context,
                                                 df_repartition_by_auction_id,
                                                 df_complex_join_with_lookups
    )
    val df_Reformat_agg_platform_video_analytics_pb =
      Reformat_agg_platform_video_analytics_pb(context, df_join_auction_data)
    Write_Proto_HDFS_agg_platform_video_analytics_pq_agg_platform_video_analytics(
      context,
      df_Reformat_agg_platform_video_analytics_pb
    )
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
                   "pipelines/second_agg_platform_video_analytics"
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
                                "pipelines/second_agg_platform_video_analytics"
    ) {
      apply(context)
    }
  }

}
