package io.prophecy.pipelines.third_agg_platform_video_analytics_hourly.config

import pureconfig._
import pureconfig.generic.ProductHint
import io.prophecy.libs._
import io.prophecy.pipelines.third_agg_platform_video_analytics_hourly.graph.Create_sup_lookup_files.config.{
  Config => Create_sup_lookup_files_Config
}

case class Config(
  var Write_Proto_HDFS_agg_platform_video_pod_analytics_pb: String = "",
  var XR_BUSINESS_DATE:                                     String = "20240724",
  var Write_Proto_HDFS_agg_platform_video_analytics_hourly_pb_agg_platform_video_analytics_hourly: String =
    "",
  var XR_LOOKUP_DATA:                                        String = "hdfs:/app_abinitio/dev",
  var XR_BUSINESS_HOUR:                                      String = "08",
  var Write_Proto_HDFS_agg_platform_video_slot_analytics_pb: String = "",
  var Read_Proto_Range_agg_platform_video_analytics_pb_agg_platform_video_analytics: String =
    "",
  var Create_sup_lookup_files: Create_sup_lookup_files_Config =
    Create_sup_lookup_files_Config(),
  var datasets: Datasets = Datasets(),
  var system:   System = System(),
  var jobs:     Option[Jobs] = None
) extends ConfigBase

object Datasets {

  implicit val confHint: ProductHint[Datasets] =
    ProductHint[Datasets](ConfigFieldMapping(CamelCase, CamelCase))

}

case class Datasets(
  var inputs:  Inputs = Inputs(),
  var outputs: Outputs = Outputs(),
  var hdfsProtoDescriptor: String =
    "hdfs:/data_team/team_user_space/prophecy-schemas-descriptor.protobin"
)

object Inputs {

  implicit val confHint: ProductHint[Inputs] =
    ProductHint[Inputs](ConfigFieldMapping(CamelCase, CamelCase))

}

case class Inputs(
  var agg_platform_video_analytics_pq: String =
    "hdfs:/data_team/team_user_space/mb475u/sample_aggs/agg_platform_video_analytics_pq",
  var sup_member_attributes_pb: String =
    "hdfs:/data_team/team_user_space/mb475u/sup/sup_member_attributes_pb",
  var sup_bidder_fx_rates: String =
    "hdfs:/data_team/team_user_space/mb475u/sup/sup_bidder_fx_rates",
  var sup_api_member_pb: String =
    "hdfs:/data_team/team_user_space/mb475u/sup/sup_api_member_pb",
  var sup_inventory_url: String =
    "hdfs:/data_team/team_user_space/mb475u/sup/sup_inventory_url",
  var sup_api_inventory_url_content_category: String =
    "hdfs:/data_team/team_user_space/mb475u/sup/sup_api_inventory_url_content_category/2024/07/24/08/sup_api_inventory_url_content_category.tsv",
  var sup_bidder_advertiser_pb: String =
    "hdfs:/data_team/team_user_space/mb475u/sup/sup_bidder_advertiser_pb"
)

object Outputs {

  implicit val confHint: ProductHint[Outputs] =
    ProductHint[Outputs](ConfigFieldMapping(CamelCase, CamelCase))

}

case class Outputs(
  var agg_platform_video_analytics_hourly_pq: String =
    "hdfs:/data_team/team_user_space/mb475u/sample_aggs/agg_platform_video_analytics_hourly_pq",
  var agg_platform_video_slot_analytics_pq: String =
    "hdfs:/data_team/team_user_space/mb475u/sample_aggs/agg_platform_video_slot_analytics_pq",
  var agg_platform_video_pod_analytics_pq: String =
    "hdfs:/data_team/team_user_space/mb475u/sample_aggs/agg_platform_video_pod_analytics_pq"
)

object System {

  implicit val confHint: ProductHint[System] =
    ProductHint[System](ConfigFieldMapping(CamelCase, CamelCase))

}

case class System(
  var startDate: String = "2024-07-24 08:00:00",
  var dateSlash: String = "2024/09/03/09",
  var ymd:       String = "willBeOverwrittenByScript",
  var ymdh:      String = "willBeOverwrittenByScript"
)

object Jobs {

  implicit val confHint: ProductHint[Jobs] =
    ProductHint[Jobs](ConfigFieldMapping(CamelCase, CamelCase))

}

case class Jobs(
  var agg_platform_video_analytics_hourly: Option[
    Agg_platform_video_analytics_hourly
  ] = None
)

object Agg_platform_video_analytics_hourly {

  implicit val confHint: ProductHint[Agg_platform_video_analytics_hourly] =
    ProductHint[Agg_platform_video_analytics_hourly](
      ConfigFieldMapping(CamelCase, CamelCase)
    )

}

case class Agg_platform_video_analytics_hourly(
  var sparkCommands:  Option[String] = None,
  var SparkArguments: Option[String] = None
)
