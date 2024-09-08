package io.prophecy.pipelines.first_agg_platform_video_analytics.graph.Main_Graph.xr_partition_key_filter_checkpointed_sort_agg_impbus_clicks.config

import io.prophecy.libs._
import pureconfig._
import pureconfig.generic.ProductHint
import org.apache.spark.sql.SparkSession

object Config {

  implicit val confHint: ProductHint[Config] =
    ProductHint[Config](ConfigFieldMapping(CamelCase, CamelCase))

}

case class Config() extends ConfigBase
case class Context(spark: SparkSession, config: Config)
