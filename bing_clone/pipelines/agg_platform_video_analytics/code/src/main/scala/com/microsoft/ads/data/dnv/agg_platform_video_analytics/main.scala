package com.microsoft.ads.data.dnv.agg_platform_video_analytics

import io.prophecy.libs._
import com.microsoft.ads.data.dnv.agg_platform_video_analytics.config._
import com.microsoft.ads.data.dnv.agg_platform_video_analytics.udfs.UDFs._
import com.microsoft.ads.data.dnv.agg_platform_video_analytics.udfs.ColumnFunctions._
import com.microsoft.ads.data.dnv.agg_platform_video_analytics.udfs.PipelineInitCode._
import com.microsoft.ads.data.dnv.agg_platform_video_analytics.graph._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Main {

  def apply(context: Context): Unit = {
    val df_Main_Graph = Main_Graph.apply(
      Main_Graph.config.Context(context.spark, context.config.Main_Graph)
    )
    val df_repartition_to_2000 =
      repartition_to_2000(context, df_Main_Graph).cache()
    val df_select_auction_data =
      select_auction_data(context, df_repartition_to_2000)
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
    val df_repartition_by_id_1 =
      repartition_by_id_1(context, df_Create_sup_lookup_files_out1).cache()
    val df_join_on_creative_ids_1 = join_on_creative_ids_1(
      context,
      df_left_outer_join_video_attributes,
      df_repartition_by_id_1,
      df_repartition_by_id_1,
      df_repartition_by_id_1,
      df_repartition_by_id_1,
      df_repartition_by_id_1,
      df_repartition_by_id_1,
      df_repartition_by_id_1
    ).cache()
    val df_repartition_by_id_and_member =
      repartition_by_id_and_member(context, df_Create_sup_lookup_files_out)
        .cache()
    val df_left_outer_join_advertisers = left_outer_join_advertisers(
      context,
      df_join_on_creative_ids_1,
      df_repartition_by_id_and_member,
      df_repartition_by_id_and_member,
      df_repartition_by_id_and_member,
      df_repartition_by_id_and_member,
      df_repartition_by_id_and_member,
      df_repartition_by_id_and_member,
      df_repartition_by_id_and_member,
      df_repartition_by_id_and_member
    )
    Script_1(context)
    val df_join_auction_data = join_auction_data(context,
                                                 df_repartition_to_2000,
                                                 df_left_outer_join_advertisers
    )
    val df_Reformat_agg_platform_video_analytics_pb =
      Reformat_agg_platform_video_analytics_pb(context, df_join_auction_data)
    Write_Proto_HDFS_agg_platform_video_analytics_pb_agg_platform_video_analytics(
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
                   "pipelines/agg_platform_video_analytics"
    )
    registerUDFs(spark)
    MetricsCollector.instrument(spark,
                                "pipelines/agg_platform_video_analytics"
    ) {
      apply(context)
    }
  }

}
