package com.microsoft.ads.data.dnv.agg_dw_clicks_pb.graph.External_Clicks.config

import io.prophecy.libs._
import pureconfig._
import pureconfig.generic.ProductHint
import org.apache.spark.sql.SparkSession
import com.microsoft.ads.data.dnv.agg_dw_clicks_pb.graph.External_Clicks.LookupToJoin20.config.{
  Config => LookupToJoin20_Config
}
import com.microsoft.ads.data.dnv.agg_dw_clicks_pb.graph.External_Clicks.LookupToJoin30.config.{
  Config => LookupToJoin30_Config
}

object Config {

  implicit val confHint: ProductHint[Config] =
    ProductHint[Config](ConfigFieldMapping(CamelCase, CamelCase))

}

case class Config(
  LookupToJoin30:   LookupToJoin30_Config = LookupToJoin30_Config(),
  LookupToJoin20:   LookupToJoin20_Config = LookupToJoin20_Config(),
  datasets:         Datasets = Datasets(),
  jobs:             Jobs = Jobs(),
  system:           System = System(),
  XR_BUSINESS_DATE: String = "20240716",
  XR_BUSINESS_HOUR: String = "17"
) extends ConfigBase

object Datasets {

  implicit val confHint: ProductHint[Datasets] =
    ProductHint[Datasets](ConfigFieldMapping(CamelCase, CamelCase))

}

case class Datasets(
  hdfsLogPrefix: String = "hdfs:/data_team/logs",
  hdfsSupPrefix: String = "hdfs:/data_team/sup",
  hdfsAggPrefix: String = "hdfs:/data_team/aggs",
  inputs:        Inputs = Inputs(),
  outputs:       Outputs = Outputs(),
  hdfsProtoDescriptor: String =
    "hdfs:/data_team/team_user_space/prophecy-schemas-descriptor.protobin"
)

object Inputs {

  implicit val confHint: ProductHint[Inputs] =
    ProductHint[Inputs](ConfigFieldMapping(CamelCase, CamelCase))

}

case class Inputs(
  log_impbus_click_pb: String = "${datasets.hdfsLogPrefix}/log_impbus_click_pb",
  log_dw_click_pb:     String = "${datasets.hdfsLogPrefix}/log_dw_click_pb",
  log_impbus_auction_event_pb: String =
    "${datasets.hdfsLogPrefix}/log_impbus_auction_event_pb",
  log_impbus_clicktracker_pb: String =
    "${datasets.hdfsLogPrefix}/log_impbus_clicktracker_pb",
  log_dw_clicktracker_pb: String =
    "${datasets.hdfsLogPrefix}/log_dw_clicktracker_pb",
  agg_dw_impressions_pb: String =
    "${datasets.hdfsAggPrefix}/agg_dw_impressions_pb",
  agg_dw_curator_impressions_pb: String =
    "${datasets.hdfsAggPrefix}/agg_dw_curator_impressions_pb",
  agg_msan_map_pb: String = "${datasets.hdfsAggPrefix}/agg_msan_map_pb",
  sup_bidder_member_sales_tax_rate_pb: String =
    "${datasets.hdfsSupPrefix}/sup_bidder_member_sales_tax_rate_pb",
  agg_dw_impressions_pq: String =
    "${datasets.hdfsAggPrefix}/agg_dw_impressions_pq"
)

object Outputs {

  implicit val confHint: ProductHint[Outputs] =
    ProductHint[Outputs](ConfigFieldMapping(CamelCase, CamelCase))

}

case class Outputs(
  agg_impbus_clicks_pb: String =
    "hdfs:/data_team/team_user_space/vm796h/target/aggs/agg_impbus_clicks_pb",
  agg_dw_clicks_pb: String =
    "hdfs:/data_team/team_user_space/vm796h/target/aggs/agg_dw_clicks_pb",
  agg_dw_curator_clicks_pb: String =
    "hdfs:/data_team/team_user_space/vm796h/target/aggs/agg_dw_curator_clicks_pb",
  agg_impbus_curator_clicks_pb: String =
    "hdfs:/data_team/team_user_space/vm796h/target/aggs/agg_impbus_curator_clicks_pb",
  agg_dw_clicktracker_pb: String =
    "hdfs:/data_team/team_user_space/vm796h/target/aggs/agg_dw_clicktracker_pb"
)

object Jobs {

  implicit val confHint: ProductHint[Jobs] =
    ProductHint[Jobs](ConfigFieldMapping(CamelCase, CamelCase))

}

case class Jobs(agg_dw_clicks_pb: Agg_dw_clicks_pb = Agg_dw_clicks_pb())

object Agg_dw_clicks_pb {

  implicit val confHint: ProductHint[Agg_dw_clicks_pb] =
    ProductHint[Agg_dw_clicks_pb](ConfigFieldMapping(CamelCase, CamelCase))

}

case class Agg_dw_clicks_pb(
  sparkArguments: Option[String] = None,
  sparkCommand:   Option[String] = None
)

object System {

  implicit val confHint: ProductHint[System] =
    ProductHint[System](ConfigFieldMapping(CamelCase, CamelCase))

}

case class System(startDate: String = "2024-07-16 17:00:00")
case class Context(spark: SparkSession, config: Config)
