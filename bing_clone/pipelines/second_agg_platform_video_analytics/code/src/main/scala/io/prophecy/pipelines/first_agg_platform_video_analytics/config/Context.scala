package io.prophecy.pipelines.first_agg_platform_video_analytics.config

import org.apache.spark.sql.SparkSession
case class Context(spark: SparkSession, config: Config)
