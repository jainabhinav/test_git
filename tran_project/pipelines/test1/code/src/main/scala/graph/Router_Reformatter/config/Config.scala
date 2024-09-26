package graph.Router_Reformatter.config

import io.prophecy.libs._
import pureconfig._
import pureconfig.generic.ProductHint
import org.apache.spark.sql.SparkSession

object Config {

  implicit val confHint: ProductHint[Config] =
    ProductHint[Config](ConfigFieldMapping(CamelCase, CamelCase))

}

case class Config(
  var XR_LOOKUP_DATA:   String = "hdfs:/app_abinitio/dev",
  var XR_BUSINESS_HOUR: String = "05",
  var XR_BUSINESS_DATE: String = "2020031"
) extends ConfigBase

case class Context(spark: SparkSession, config: Config)
