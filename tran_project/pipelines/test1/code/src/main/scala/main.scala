import io.prophecy.libs._
import config._
import udfs.UDFs._
import udfs.ColumnFunctions._
import udfs.PipelineInitCode._
import graph._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Main {

  def apply(context: Context): Unit = {
    val (df_Create_sup_lookup_files_out11,
         df_Create_sup_lookup_files_out10,
         df_Create_sup_lookup_files_out9,
         df_Create_sup_lookup_files_out8,
         df_Create_sup_lookup_files_out7,
         df_Create_sup_lookup_files_out6,
         df_Create_sup_lookup_files_out5,
         df_Create_sup_lookup_files_out4,
         df_Create_sup_lookup_files_out3,
         df_Create_sup_lookup_files_out2,
         df_Create_sup_lookup_files_out1,
         df_Create_sup_lookup_files_out
    ) = Create_sup_lookup_files.apply(
      Create_sup_lookup_files.config
        .Context(context.spark, context.config.Create_sup_lookup_files)
    )
    Script_Config(context)
    val df_Read_Proto_Range_log_impbus_view_pb_log_impbus_view =
      Read_Proto_Range_log_impbus_view_pb_log_impbus_view(context)
    val df_xr_partition_key_filter_checkpointed_sort_log_impbus_view_auction_id_date_time =
      xr_partition_key_filter_checkpointed_sort_log_impbus_view_auction_id_date_time
        .apply(
          xr_partition_key_filter_checkpointed_sort_log_impbus_view_auction_id_date_time.config
            .Context(
              context.spark,
              context.config.xr_partition_key_filter_checkpointed_sort_log_impbus_view_auction_id_date_time
            ),
          df_Read_Proto_Range_log_impbus_view_pb_log_impbus_view
        )
    val df_Rollup_log_impbus_view = Rollup_log_impbus_view(
      context,
      df_xr_partition_key_filter_checkpointed_sort_log_impbus_view_auction_id_date_time
    )
    val df_Rollup_log_impbus_view_results =
      Rollup_log_impbus_view_results(context, df_Rollup_log_impbus_view)
    val df_Rollup_log_impbus_view_Reformat = Rollup_log_impbus_view_Reformat(
      context,
      df_Rollup_log_impbus_view_results
    )
    val df_Read_Proto_Range_log_impbus_auction_event_pb_log_impbus_auction_event =
      Read_Proto_Range_log_impbus_auction_event_pb_log_impbus_auction_event(
        context
      )
    val df_xr_partition_key_filter_checkpointed_sort_log_impbus_auction_event_auction_id_date_time =
      xr_partition_key_filter_checkpointed_sort_log_impbus_auction_event_auction_id_date_time
        .apply(
          xr_partition_key_filter_checkpointed_sort_log_impbus_auction_event_auction_id_date_time.config
            .Context(
              context.spark,
              context.config.xr_partition_key_filter_checkpointed_sort_log_impbus_auction_event_auction_id_date_time
            ),
          df_Read_Proto_Range_log_impbus_auction_event_pb_log_impbus_auction_event
        )
    val df_Rollup_log_impbus_auction_event = Rollup_log_impbus_auction_event(
      context,
      df_xr_partition_key_filter_checkpointed_sort_log_impbus_auction_event_auction_id_date_time
    )
    val df_Rollup_log_impbus_auction_event_Reformat =
      Rollup_log_impbus_auction_event_Reformat(
        context,
        df_Rollup_log_impbus_auction_event
      )
    val df_Read_Proto_Range_log_dw_bid_pb_log_dw_bid =
      Read_Proto_Range_log_dw_bid_pb_log_dw_bid(context)
    val df_Read_Proto_Range_log_dw_imptracker_pb_log_dw_imptracker =
      Read_Proto_Range_log_dw_imptracker_pb_log_dw_imptracker(context)
    val df_Reformat_log_dw_bid_PrevExpression =
      Reformat_log_dw_bid_PrevExpression(
        context,
        df_Read_Proto_Range_log_dw_imptracker_pb_log_dw_imptracker
      )
    val df_Reformat_log_dw_bid =
      Reformat_log_dw_bid(context, df_Reformat_log_dw_bid_PrevExpression)
    val df_xr_partition_key_filter_checkpointed_sort_log_dw_bid_auction_id_date_time =
      xr_partition_key_filter_checkpointed_sort_log_dw_bid_auction_id_date_time
        .apply(
          xr_partition_key_filter_checkpointed_sort_log_dw_bid_auction_id_date_time.config
            .Context(
              context.spark,
              context.config.xr_partition_key_filter_checkpointed_sort_log_dw_bid_auction_id_date_time
            ),
          df_Read_Proto_Range_log_dw_bid_pb_log_dw_bid,
          df_Reformat_log_dw_bid
        )
    val df_Rollup_log_dw_bid = Rollup_log_dw_bid(
      context,
      df_xr_partition_key_filter_checkpointed_sort_log_dw_bid_auction_id_date_time
    )
    val df_Rollup_log_dw_bid_Reformat =
      Rollup_log_dw_bid_Reformat(context, df_Rollup_log_dw_bid)
    val df_Read_Proto_Range_log_dw_view_pb_log_dw_view =
      Read_Proto_Range_log_dw_view_pb_log_dw_view(context)
    val df_xr_partition_key_filter_checkpointed_sort_log_dw_view_auction_id_date_time =
      xr_partition_key_filter_checkpointed_sort_log_dw_view_auction_id_date_time
        .apply(
          xr_partition_key_filter_checkpointed_sort_log_dw_view_auction_id_date_time.config
            .Context(
              context.spark,
              context.config.xr_partition_key_filter_checkpointed_sort_log_dw_view_auction_id_date_time
            ),
          df_Read_Proto_Range_log_dw_view_pb_log_dw_view
        )
    val df_Rollup_log_dw_view = Rollup_log_dw_view(
      context,
      df_xr_partition_key_filter_checkpointed_sort_log_dw_view_auction_id_date_time
    )
    val df_Rollup_log_dw_view_Reformat =
      Rollup_log_dw_view_Reformat(context, df_Rollup_log_dw_view)
    val df_Read_Proto_Range_video_slot_pb_video_slot_5 =
      Read_Proto_Range_video_slot_pb_video_slot_5(context)
    val df_Partition_by_Key_video_slot_auction_id =
      Partition_by_Key_video_slot_auction_id(
        context,
        df_Read_Proto_Range_video_slot_pb_video_slot_5
      )
    val df_Filter_by_Expression_video_slot = Filter_by_Expression_video_slot(
      context,
      df_Partition_by_Key_video_slot_auction_id
    )
    val df_Checkpointed_Sort_video_slot_auction_id_64__Partial_Sort =
      Checkpointed_Sort_video_slot_auction_id_64__Partial_Sort(
        context,
        df_Filter_by_Expression_video_slot
      )
    val df_Rollup_video_slot = Rollup_video_slot(
      context,
      df_Checkpointed_Sort_video_slot_auction_id_64__Partial_Sort
    )
    val df_Rollup_video_slot_Reformat =
      Rollup_video_slot_Reformat(context, df_Rollup_video_slot)
    val df_Read_Proto_Range_log_impbus_imptracker_pb_log_impbus_imptracker =
      Read_Proto_Range_log_impbus_imptracker_pb_log_impbus_imptracker(context)
    val df_Reformat_log_impbus_impressions_PrevExpression =
      Reformat_log_impbus_impressions_PrevExpression(
        context,
        df_Read_Proto_Range_log_impbus_imptracker_pb_log_impbus_imptracker
      )
    val df_Reformat_log_impbus_impressions = Reformat_log_impbus_impressions(
      context,
      df_Reformat_log_impbus_impressions_PrevExpression
    )
    val df_Partition_by_Key_log_impbus_impressions_auction_id_UnionAll_normalize_schema_2 =
      Partition_by_Key_log_impbus_impressions_auction_id_UnionAll_normalize_schema_2(
        context,
        df_Reformat_log_impbus_impressions
      )
    val df_Read_Proto_Range_log_impbus_impressions_deferred_pb_log_impbus_impressions_5 =
      Read_Proto_Range_log_impbus_impressions_deferred_pb_log_impbus_impressions_5(
        context
      )
    val df_Partition_by_Key_log_impbus_impressions_auction_id_UnionAll_normalize_schema_1 =
      Partition_by_Key_log_impbus_impressions_auction_id_UnionAll_normalize_schema_1(
        context,
        df_Read_Proto_Range_log_impbus_impressions_deferred_pb_log_impbus_impressions_5
      )
    val df_Read_Proto_Range_log_impbus_impressions_pb_log_impbus_impressions =
      Read_Proto_Range_log_impbus_impressions_pb_log_impbus_impressions(context)
    val df_Partition_by_Key_log_impbus_impressions_auction_id_UnionAll_normalize_schema_0 =
      Partition_by_Key_log_impbus_impressions_auction_id_UnionAll_normalize_schema_0(
        context,
        df_Read_Proto_Range_log_impbus_impressions_pb_log_impbus_impressions
      )
    val df_Partition_by_Key_log_impbus_impressions_auction_id_UnionAll =
      Partition_by_Key_log_impbus_impressions_auction_id_UnionAll(
        context,
        df_Partition_by_Key_log_impbus_impressions_auction_id_UnionAll_normalize_schema_2,
        df_Partition_by_Key_log_impbus_impressions_auction_id_UnionAll_normalize_schema_1,
        df_Partition_by_Key_log_impbus_impressions_auction_id_UnionAll_normalize_schema_0
      )
    val df_Partition_by_Key_log_impbus_impressions_auction_id =
      Partition_by_Key_log_impbus_impressions_auction_id(
        context,
        df_Partition_by_Key_log_impbus_impressions_auction_id_UnionAll
      )
    val df_Filter_by_Expression_log_impbus_impressions =
      Filter_by_Expression_log_impbus_impressions(
        context,
        df_Partition_by_Key_log_impbus_impressions_auction_id
      )
    val df_Rollup_log_impbus_impressions = Rollup_log_impbus_impressions(
      context,
      df_Filter_by_Expression_log_impbus_impressions
    )
    val df_Rollup_log_impbus_impressions_Reformat =
      Rollup_log_impbus_impressions_Reformat(context,
                                             df_Rollup_log_impbus_impressions
      )
    val df_Read_Proto_Range_log_impbus_preempt_pb_log_impbus_preempt =
      Read_Proto_Range_log_impbus_preempt_pb_log_impbus_preempt(context)
    val df_Read_Proto_Range_log_impbus_preempt_deferred_pb_log_impbus_preempt_deferred_5 =
      Read_Proto_Range_log_impbus_preempt_deferred_pb_log_impbus_preempt_deferred_5(
        context
      )
    val df_xr_partition_key_filter_checkpointed_sort_log_impbus_preempt_auction_id_date_time =
      xr_partition_key_filter_checkpointed_sort_log_impbus_preempt_auction_id_date_time
        .apply(
          xr_partition_key_filter_checkpointed_sort_log_impbus_preempt_auction_id_date_time.config
            .Context(
              context.spark,
              context.config.xr_partition_key_filter_checkpointed_sort_log_impbus_preempt_auction_id_date_time
            ),
          df_Read_Proto_Range_log_impbus_preempt_pb_log_impbus_preempt,
          df_Read_Proto_Range_log_impbus_preempt_deferred_pb_log_impbus_preempt_deferred_5
        )
    val df_Rollup1 = Rollup1(
      context,
      df_xr_partition_key_filter_checkpointed_sort_log_impbus_preempt_auction_id_date_time
    )
    val df_Rollup_log_impbus_preempt_Reformat =
      Rollup_log_impbus_preempt_Reformat(context, df_Rollup1)
    val df_Left_Outer_Join_log_impbus_preeempt =
      Left_Outer_Join_log_impbus_preeempt(
        context,
        df_Rollup_log_impbus_impressions_Reformat,
        df_Rollup_log_impbus_preempt_Reformat
      )
    val (df_Reformat_0_is_not_transactable_and_is_not_deferred_impression_1_is_transactable_and_is_not_transacted_previously_RowDistributor_out0,
         df_Reformat_0_is_not_transactable_and_is_not_deferred_impression_1_is_transactable_and_is_not_transacted_previously_RowDistributor_out1
    ) =
      Reformat_0_is_not_transactable_and_is_not_deferred_impression_1_is_transactable_and_is_not_transacted_previously_RowDistributor(
        context,
        df_Left_Outer_Join_log_impbus_preeempt
      )
    val df_Reformat_0_is_not_transactable_and_is_not_deferred_impression_1_is_transactable_and_is_not_transacted_previouslyReformat_0 =
      Reformat_0_is_not_transactable_and_is_not_deferred_impression_1_is_transactable_and_is_not_transacted_previouslyReformat_0(
        context,
        df_Reformat_0_is_not_transactable_and_is_not_deferred_impression_1_is_transactable_and_is_not_transacted_previously_RowDistributor_out0
      )
    val df_Merge_UnionAll_normalize_schema_1 =
      Merge_UnionAll_normalize_schema_1(
        context,
        df_Reformat_0_is_not_transactable_and_is_not_deferred_impression_1_is_transactable_and_is_not_transacted_previouslyReformat_0
      )
    val df_Reformat_0_is_not_transactable_and_is_not_deferred_impression_1_is_transactable_and_is_not_transacted_previouslyReformat_1 =
      Reformat_0_is_not_transactable_and_is_not_deferred_impression_1_is_transactable_and_is_not_transacted_previouslyReformat_1(
        context,
        df_Reformat_0_is_not_transactable_and_is_not_deferred_impression_1_is_transactable_and_is_not_transacted_previously_RowDistributor_out1
      )
    val df_Read_Proto_Range_log_impbus_impressions_pricing_pb_log_impbus_impressions_pricing =
      Read_Proto_Range_log_impbus_impressions_pricing_pb_log_impbus_impressions_pricing(
        context
      )
    val df_Read_Proto_Range_log_impbus_impressions_deferred_pricing_pb_log_impbus_impressions_pricing_5 =
      Read_Proto_Range_log_impbus_impressions_deferred_pricing_pb_log_impbus_impressions_pricing_5(
        context
      )
    val df_Reformat_log_impbus_impressions_pricing_PrevExpression =
      Reformat_log_impbus_impressions_pricing_PrevExpression(
        context,
        df_Read_Proto_Range_log_impbus_imptracker_pb_log_impbus_imptracker
      )
    val df_Reformat_log_impbus_impressions_pricing =
      Reformat_log_impbus_impressions_pricing(
        context,
        df_Reformat_log_impbus_impressions_pricing_PrevExpression
      )
    val df_xr_partition_key_filter_checkpointed_sort_log_impbus_impressions_pricing_auction_id_date_time =
      xr_partition_key_filter_checkpointed_sort_log_impbus_impressions_pricing_auction_id_date_time
        .apply(
          xr_partition_key_filter_checkpointed_sort_log_impbus_impressions_pricing_auction_id_date_time.config
            .Context(
              context.spark,
              context.config.xr_partition_key_filter_checkpointed_sort_log_impbus_impressions_pricing_auction_id_date_time
            ),
          df_Read_Proto_Range_log_impbus_impressions_pricing_pb_log_impbus_impressions_pricing,
          df_Read_Proto_Range_log_impbus_impressions_deferred_pricing_pb_log_impbus_impressions_pricing_5,
          df_Reformat_log_impbus_impressions_pricing
        )
    val df_Rollup_log_impbus_impressions_pricing =
      Rollup_log_impbus_impressions_pricing(
        context,
        df_xr_partition_key_filter_checkpointed_sort_log_impbus_impressions_pricing_auction_id_date_time
      )
    val df_Rollup_log_impbus_impressions_pricing_Reformat =
      Rollup_log_impbus_impressions_pricing_Reformat(
        context,
        df_Rollup_log_impbus_impressions_pricing
      )
    val df_Inner_Join_log_impbus_impressions_pricing =
      Inner_Join_log_impbus_impressions_pricing(
        context,
        df_Reformat_0_is_not_transactable_and_is_not_deferred_impression_1_is_transactable_and_is_not_transacted_previouslyReformat_1,
        df_Rollup_log_impbus_impressions_pricing_Reformat
      )
    val df_Merge_UnionAll_normalize_schema_0 =
      Merge_UnionAll_normalize_schema_0(
        context,
        df_Inner_Join_log_impbus_impressions_pricing
      )
    val df_Merge_UnionAll = Merge_UnionAll(context,
                                           df_Merge_UnionAll_normalize_schema_1,
                                           df_Merge_UnionAll_normalize_schema_0
    )
    val df_Merge           = Merge(context,           df_Merge_UnionAll)
    val df_Filter_UnitTest = Filter_UnitTest(context, df_Merge)
    val df_Left_Outer_Join_log_impbus_impressions =
      Left_Outer_Join_log_impbus_impressions(
        context,
        df_Filter_UnitTest,
        df_Rollup_log_impbus_view_Reformat,
        df_Rollup_log_impbus_auction_event_Reformat,
        df_Rollup_log_dw_bid_Reformat,
        df_Rollup_log_dw_view_Reformat,
        df_Rollup_video_slot_Reformat
      )
    val df_Reformat_select_log_dw_bid = Reformat_select_log_dw_bid(
      context,
      df_Left_Outer_Join_log_impbus_impressions
    )
    val df_Checkpoint_1 = Checkpoint_1(context, df_Reformat_select_log_dw_bid)
    val (df_Router_Reformatter_adi,
         df_Router_Reformatter_curated,
         df_Router_Reformatter_out2,
         df_Router_Reformatter_quarantine,
         df_Router_Reformatter_ssd
    ) = Router_Reformatter.apply(
      Router_Reformatter.config
        .Context(context.spark, context.config.Router_Reformatter),
      df_Create_sup_lookup_files_out6,
      df_Create_sup_lookup_files_out7,
      df_Create_sup_lookup_files_out8,
      df_Create_sup_lookup_files_out9,
      df_Create_sup_lookup_files_out10,
      df_Create_sup_lookup_files_out11,
      df_Create_sup_lookup_files_out4,
      df_Create_sup_lookup_files_out1,
      df_Create_sup_lookup_files_out2,
      df_Create_sup_lookup_files_out,
      df_Checkpoint_1
    )
    val df_Gather_SSPQ_normalize_schema_0 =
      Gather_SSPQ_normalize_schema_0(context, df_Router_Reformatter_ssd)
    val df_select_auction_data_1 =
      select_auction_data_1(context, df_Router_Reformatter_quarantine)
    Write_Proto_HDFS_IIPQ_stage_invalid_impressions_quarantine_pb_stage_invalid_impressions_quarantine(
      context,
      df_select_auction_data_1
    )
    val df_auction_data_projection =
      auction_data_projection(context, df_Router_Reformatter_ssd)
    val df_Filter_by_Expression_agg_dw_curator_impressionsj0 =
      Filter_by_Expression_agg_dw_curator_impressionsj0(
        context,
        df_Router_Reformatter_curated,
        df_Create_sup_lookup_files_out,
        df_Create_sup_lookup_files_out1,
        df_Create_sup_lookup_files_out2
      )
    val df_Filter_by_Expression_agg_dw_curator_impressions =
      Filter_by_Expression_agg_dw_curator_impressions(
        context,
        df_Filter_by_Expression_agg_dw_curator_impressionsj0
      )
    val df_Filter_by_Expression_agg_dw_curator_impressions_DropExtraColumns =
      Filter_by_Expression_agg_dw_curator_impressions_DropExtraColumns(
        context,
        df_Filter_by_Expression_agg_dw_curator_impressions
      )
    val df_select_auction_data = select_auction_data(
      context,
      df_Filter_by_Expression_agg_dw_curator_impressions_DropExtraColumns
    )
    Write_Proto_HDFS_agg_dw_curator_impressions_pb_agg_dw_curator_impressions(
      context,
      df_select_auction_data
    )
    val df_Gather_SSPQ_normalize_schema_1 =
      Gather_SSPQ_normalize_schema_1(context, df_Router_Reformatter_out2)
    val df_Gather_SSPQ = Gather_SSPQ(context,
                                     df_Gather_SSPQ_normalize_schema_1,
                                     df_Gather_SSPQ_normalize_schema_0
    )
    val df_Filter_by_Expression_Sample_SSD =
      Filter_by_Expression_Sample_SSD(context, df_Gather_SSPQ)
    val df_Reformat_stage_impbus_impression_sample =
      Reformat_stage_impbus_impression_sample(context,
                                              df_Filter_by_Expression_Sample_SSD
      )
    Write_Proto_HDFS_stage_impbus_impression_sample_stage_impbus_impression_sample_trans_path_pb(
      context,
      df_Reformat_stage_impbus_impression_sample
    )
    val df_Normalize = Normalize(context, df_Router_Reformatter_adi)
    val df_Normalize_outputReformat =
      Normalize_outputReformat(context, df_Normalize)
    val df_LookupToJoin50 = LookupToJoin50.apply(
      LookupToJoin50.config
        .Context(context.spark, context.config.LookupToJoin50),
      df_Normalize_outputReformat,
      df_Create_sup_lookup_files_out3,
      df_Create_sup_lookup_files_out4,
      df_Create_sup_lookup_files_out5
    )
    val df_Reformat_agg_dw_impressions_args_PreCompute =
      Reformat_agg_dw_impressions_args_PreCompute(context, df_LookupToJoin50)
    val df_auction_data_projection_1 =
      auction_data_projection_1(context, df_Gather_SSPQ)
    if (false)
      dbg_tran_leftjoin(context, df_Reformat_select_log_dw_bid)
    val df_Checkpoint =
      Checkpoint(context, df_Reformat_agg_dw_impressions_args_PreCompute)
    val df_Reformat_agg_dw_impressions_PrevExpression =
      Reformat_agg_dw_impressions_PrevExpression(context, df_Checkpoint)
    val df_checkpoint_dataframe = checkpoint_dataframe(
      context,
      df_Reformat_agg_dw_impressions_PrevExpression
    )
    val df_Reformat_agg_dw_impressions =
      Reformat_agg_dw_impressions(context, df_checkpoint_dataframe)
    val df_Filter_by_Expression_agg_dw_impressionsj0 =
      Filter_by_Expression_agg_dw_impressionsj0(context,
                                                df_Reformat_agg_dw_impressions,
                                                df_Create_sup_lookup_files_out,
                                                df_Create_sup_lookup_files_out1,
                                                df_Create_sup_lookup_files_out2
      )
    val df_Filter_by_Expression_agg_dw_impressions =
      Filter_by_Expression_agg_dw_impressions(
        context,
        df_Filter_by_Expression_agg_dw_impressionsj0
      )
    val df_Filter_by_Expression_agg_dw_impressions_DropExtraColumns =
      Filter_by_Expression_agg_dw_impressions_DropExtraColumns(
        context,
        df_Filter_by_Expression_agg_dw_impressions
      )
    val df_full_data_projection = full_data_projection(
      context,
      df_Filter_by_Expression_agg_dw_impressions_DropExtraColumns
    )
    Write_Proto_HDFS_ADI_agg_dw_impressions_pb_agg_dw_impressions(
      context,
      df_full_data_projection
    )
    val df_target_starget_impbus_impression_sample_trans_path_pb =
      target_starget_impbus_impression_sample_trans_path_pb(context)
    Write_Proto_HDFS_SSPQ_stage_tl_trx_trans_denormalized_pb_stage_seen_denormalized(
      context,
      df_auction_data_projection_1
    )
    val df_Filter_UnitTest_1 = Filter_UnitTest_1(
      context,
      df_Partition_by_Key_log_impbus_impressions_auction_id_UnionAll_normalize_schema_2
    )
    val df_Reformat_agg_dw_impressions_member_info =
      Reformat_agg_dw_impressions_member_info(
        context,
        df_Filter_by_Expression_agg_dw_impressions_DropExtraColumns
      )
    val df_Rollup_agg_dw_impressions_member_info_local =
      Rollup_agg_dw_impressions_member_info_local(
        context,
        df_Reformat_agg_dw_impressions_member_info
      )
    val df_Rollup_agg_dw_impressions_member_info_local_Reformat =
      Rollup_agg_dw_impressions_member_info_local_Reformat(
        context,
        df_Rollup_agg_dw_impressions_member_info_local
      )
    val df_Rollup_agg_dw_impressions_member_info_global =
      Rollup_agg_dw_impressions_member_info_global(
        context,
        df_Rollup_agg_dw_impressions_member_info_local_Reformat
      )
    val df_Rollup_agg_dw_impressions_member_info_global_Reformat =
      Rollup_agg_dw_impressions_member_info_global_Reformat(
        context,
        df_Rollup_agg_dw_impressions_member_info_global
      )
    val df_Reformat_Filter_Member_Counts = Reformat_Filter_Member_Counts(
      context,
      df_Rollup_agg_dw_impressions_member_info_global_Reformat
    )
    Write_Multiple_Files_memberCounts(context, df_Reformat_Filter_Member_Counts)
    Write_Proto_HDFS_stage_seen_transacted_pb_stage_seen_denormalized(
      context,
      df_auction_data_projection
    )
    val df_target_agg_dw_impressions_pb = target_agg_dw_impressions_pb(context)
    val df_stage_invalid_impressions_quarantine =
      stage_invalid_impressions_quarantine(context)
    val df_target_agg_dw_curator_impressions_pb =
      target_agg_dw_curator_impressions_pb(context)
    val df_pb_stage_seen_denormalized = pb_stage_seen_denormalized(context)
    val df_stage_tl_trx_trans_denormalized = stage_tl_trx_trans_denormalized(
      context
    )
  }

  def main(args: Array[String]): Unit = {
    val config = ConfigurationFactoryImpl.getConfig(args)
    val spark: SparkSession = SparkSession
      .builder()
      .appName("Prophecy Pipeline")
      .config("spark.default.parallelism",             "4")
      .config("spark.sql.legacy.allowUntypedScalaUDF", "true")
      .enableHiveSupport()
      .getOrCreate()
    val context = Context(spark, config)
    spark.conf.set("prophecy.metadata.pipeline.uri",        "pipelines/test1")
    spark.conf.set("spark.sql.legacy.allowUntypedScalaUDF", "true")
    spark.conf.set("spark.sql.legacy.timeParserPolicy",     "LEGACY")
    spark.conf.set("spark.sql.objectHashAggregate.sortBased.fallbackThreshold",
                   "100000"
    )
    spark.conf.set("spark.sql.broadcastTimeout",               "1d")
    spark.conf.set("spark.storage.blockManagerSlaveTimeoutMs", "5d")
    registerUDFs(spark)
    MetricsCollector.instrument(spark, "pipelines/test1") {
      apply(context)
    }
  }

}
