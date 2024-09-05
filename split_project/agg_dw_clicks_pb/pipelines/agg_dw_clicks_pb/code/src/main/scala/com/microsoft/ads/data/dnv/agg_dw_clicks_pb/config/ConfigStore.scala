package com.microsoft.ads.data.dnv.agg_dw_clicks_pb.config

import com.microsoft.ads.data.dnv.agg_dw_clicks_pb.config.Context
import pureconfig.ConfigReader.Result
import pureconfig._
import pureconfig.generic.ProductHint
import pureconfig.generic.auto._
import io.prophecy.libs._
object ConfigStore

object ConfigurationFactoryImpl extends ConfigurationFactory[Config] {

  override protected def load(
    fileConfig: ConfigObjectSource
  ): Result[Config] = {
    implicit val confHint: ProductHint[Config] =
      ProductHint[Config](ConfigFieldMapping(CamelCase, CamelCase))
    fileConfig.load[Config]
  }

  def getConfig(args: Array[String]) =
    fromCLI(args, "agg_dw_clicks_pb.conf", "config")

}
