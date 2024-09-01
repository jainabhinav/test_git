package com.microsoft.ads.data.dnv.agg_platform_video_analytics.config

import org.apache.spark.sql.SparkSession
case class Context(spark: SparkSession, config: Config)
