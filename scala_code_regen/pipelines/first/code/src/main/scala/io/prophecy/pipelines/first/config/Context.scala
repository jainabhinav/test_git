package io.prophecy.pipelines.first.config

import org.apache.spark.sql.SparkSession
case class Context(spark: SparkSession, config: Config)
