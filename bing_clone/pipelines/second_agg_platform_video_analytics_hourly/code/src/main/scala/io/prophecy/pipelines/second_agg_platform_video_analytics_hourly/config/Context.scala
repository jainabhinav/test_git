package io.prophecy.pipelines.second_agg_platform_video_analytics_hourly.config

import org.apache.spark.sql.SparkSession
case class Context(spark: SparkSession, config: Config)
