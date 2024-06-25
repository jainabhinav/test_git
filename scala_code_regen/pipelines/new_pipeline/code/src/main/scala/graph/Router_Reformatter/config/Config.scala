package graph.Router_Reformatter.config

import io.prophecy.libs._
import pureconfig._
import pureconfig.generic.ProductHint
import org.apache.spark.sql.SparkSession
import graph.Router_Reformatter.LookupToJoin70.config.{
  Config => LookupToJoin70_Config
}
import graph.Router_Reformatter.LookupToJoin00.config.{
  Config => LookupToJoin00_Config
}
import graph.Router_Reformatter.LookupToJoin10.config.{
  Config => LookupToJoin10_Config
}
import graph.Router_Reformatter.LookupToJoin20.config.{
  Config => LookupToJoin20_Config
}
import graph.Router_Reformatter.LookupToJoin40.config.{
  Config => LookupToJoin40_Config
}

object Config {

  implicit val confHint: ProductHint[Config] =
    ProductHint[Config](ConfigFieldMapping(CamelCase, CamelCase))

}

case class Config(
  XR_LOOKUP_DATA:   String = "hdfs:/app_abinitio/dev",
  XR_BUSINESS_HOUR: String = "10",
  XR_BUSINESS_DATE: String = "20190101",
  LookupToJoin70:   LookupToJoin70_Config = LookupToJoin70_Config(),
  LookupToJoin20:   LookupToJoin20_Config = LookupToJoin20_Config(),
  LookupToJoin10:   LookupToJoin10_Config = LookupToJoin10_Config(),
  LookupToJoin00:   LookupToJoin00_Config = LookupToJoin00_Config(),
  LookupToJoin40:   LookupToJoin40_Config = LookupToJoin40_Config()
) extends ConfigBase

case class Context(spark: SparkSession, config: Config)
