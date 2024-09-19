package io.prophecy.pipelines.first_agg_platform_video_analytics.graph.Create_sup_lookup_files.config

import io.prophecy.libs._
import pureconfig._
import pureconfig.generic.ProductHint
import org.apache.spark.sql.SparkSession

object Config {

  implicit val confHint: ProductHint[Config] =
    ProductHint[Config](ConfigFieldMapping(CamelCase, CamelCase))

}

case class Config(
  var Create_sup_lookup_files_Read_Proto_Range_sup_bidder_fx_rates: String = "",
  var XR_BUSINESS_DATE:                                             String = "willBeOverwrittenByScript",
  var Create_sup_lookup_files_Read_Proto_Range_sup_placement_video_attributes_pb: String =
    "",
  var XR_LOOKUP_DATA:   String = "hdfs:/app_abinitio/dev",
  var XR_BUSINESS_HOUR: String = "willBeOverwrittenByScript",
  var Create_sup_lookup_files_Read_DML_Range_sup_api_member_tsv_sup_api_member: String =
    "",
  var Create_sup_lookup_files_Read_Proto_Range_sup_creative_media_subtype_pb: String =
    "",
  var Create_sup_lookup_files_Read_Proto_Range_sup_video_completion_rate_prediction_pb: String =
    "",
  var Create_sup_lookup_files_Read_Proto_Range_sup_bidder_advertiser_pb: String =
    "",
  var Create_sup_lookup_files_Read_Proto_Range_sup_code_fx_rate:  String = "",
  var Create_sup_lookup_files_Read_Proto_Range_sup_api_member_pb: String = "",
  var datasets:                                                   Datasets = Datasets(),
  var system:                                                     System = System()
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
  var sup_api_member_pb: String =
    "hdfs:/data_team/team_user_space/mx429j1/prophecy/sample/sup/sup_api_member_pb",
  var sup_bidder_advertiser_pb: String =
    "hdfs:/data_team/team_user_space/mx429j1/prophecy/sample/sup/sup_bidder_advertiser_pb",
  var sup_bidder_fx_rates_pb: String =
    "hdfs:/data_team/team_user_space/mx429j1/prophecy/sample/sup/sup_bidder_fx_rates",
  var sup_creative_media_subtype_pb: String =
    "hdfs:/data_team/team_user_space/mx429j1/prophecy/sample/sup/sup_creative_media_subtype_pb",
  var sup_placement_video_attributes_pb: String =
    "hdfs:/data_team/team_user_space/mx429j1/prophecy/sample/sup/sup_placement_video_attributes_pb",
  var agg_dw_video_events_pb: String =
    "hdfs:/data_team/team_user_space/mx429j1/prophecy/sample/aggs/agg_dw_video_events_pb",
  var agg_platform_video_impressions_pb: String =
    "hdfs:/data_team/team_user_space/mx429j1/prophecy/sample/aggs/agg_platform_video_impressions_pb",
  var agg_dw_clicks_pb: String =
    "hdfs:/data_team/team_user_space/mx429j1/prophecy/sample/aggs/agg_dw_clicks_pb",
  var agg_dw_pixels_pb: String =
    "hdfs:/data_team/team_user_space/mx429j1/prophecy/sample/aggs/agg_dw_pixels_pb",
  var agg_impbus_clicks_pb: String =
    "hdfs:/data_team/team_user_space/mx429j1/prophecy/sample/aggs/agg_impbus_clicks_pb",
  var agg_platform_video_requests_pq: String =
    "hdfs:/data_team/team_user_space/mx429j1/prophecy/sample/aggs/agg_platform_video_requests_pq",
  var agg_platform_video_requests_transactable_pq: String =
    "hdfs:/data_team/team_user_space/mx429j1/prophecy/sample/aggs/agg_platform_video_requests_transactable_pq",
  var agg_platform_video_requests_pb: String =
    "hdfs:/data_team/team_user_space/mx429j1/prophecy/sample/aggs/agg_platform_video_requests_pb",
  var agg_platform_video_requests_transactable_pb: String =
    "hdfs:/data_team/team_user_space/mx429j1/prophecy/sample/aggs/agg_platform_video_requests_transactable_pb",
  var agg_platform_video_impressions_pq: String =
    "hdfs:/data_team/team_user_space/mx429j1/prophecy/sample/aggs/agg_platform_video_impressions_pq",
  var agg_dw_video_events_pq: String =
    "hdfs:/data_team/team_user_space/mx429j1/prophecy/sample/aggs/agg_dw_video_events_pq",
  var agg_dw_pixels_pq: String =
    "hdfs:/data_team/team_user_space/mx429j1/prophecy/sample/aggs/agg_dw_pixels_pq",
  var agg_impbus_clicks_pq: String =
    "hdfs:/data_team/team_user_space/mx429j1/prophecy/sample/aggs/agg_impbus_clicks_pq",
  var agg_dw_clicks_pq: String =
    "hdfs:/data_team/team_user_space/mx429j1/prophecy/sample/aggs/agg_dw_clicks_pq"
)

object Outputs {

  implicit val confHint: ProductHint[Outputs] =
    ProductHint[Outputs](ConfigFieldMapping(CamelCase, CamelCase))

}

case class Outputs(
  var agg_platform_video_analytics_pb: String =
    "hdfs:/data_team/team_user_space/mx429j1/prophecy/aggs/agg_platform_video_analytics_pb",
  var agg_platform_video_analytics_pq: String =
    "hdfs:/data_team/team_user_space/mx429j1/prophecy/aggs/agg_platform_video_analytics_pq"
)

object System {

  implicit val confHint: ProductHint[System] =
    ProductHint[System](ConfigFieldMapping(CamelCase, CamelCase))

}

case class System(var startDate: String = "2024-09-01 15:00:00")
case class Context(spark: SparkSession, config: Config)
