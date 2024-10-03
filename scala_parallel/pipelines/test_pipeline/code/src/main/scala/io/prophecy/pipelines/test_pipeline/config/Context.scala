package io.prophecy.pipelines.test_pipeline.config

import org.apache.spark.sql.SparkSession
import io.prophecy.libs.MonitoringContext.Monitoring
case class Context(spark: SparkSession, config: Config) extends Monitoring
