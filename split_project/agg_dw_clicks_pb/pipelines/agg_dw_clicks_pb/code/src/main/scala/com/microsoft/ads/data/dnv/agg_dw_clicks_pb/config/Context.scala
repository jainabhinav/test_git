package com.microsoft.ads.data.dnv.agg_dw_clicks_pb.config

import org.apache.spark.sql.SparkSession
case class Context(spark: SparkSession, config: Config)
