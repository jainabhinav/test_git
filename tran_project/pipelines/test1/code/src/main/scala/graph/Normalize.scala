package graph

import io.prophecy.libs._
import udfs.PipelineInitCode._
import udfs.UDFs._
import udfs.ColumnFunctions._
import config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Normalize {

  def apply(context: Context, in: DataFrame): DataFrame = {
    import org.apache.spark.sql.ProphecyDataFrame
    import org.apache.spark.sql.functions.lit
    ProphecyDataFrame
      .extendedDataFrame(in)
      .normalize(
        Some(
          coalesce(
            when(
              (col("imp_type").cast(IntegerType) === lit(5))
                .or(col("imp_type").cast(IntegerType) === lit(7))
                .or(col("imp_type").cast(IntegerType) === lit(9)),
              when(is_not_null(col("log_dw_bid")).cast(BooleanType), lit(1))
                .otherwise(lit(0))
            ),
            when(col("imp_type").cast(IntegerType) === lit(6),
                 lit(1) + when(is_not_null(col("log_dw_bid")).cast(BooleanType),
                               lit(1)
                 ).otherwise(lit(0))
            ),
            lit(1)
          )
        ),
        None,
        None,
        "index",
        List(member_id(context).as("member_id"),
             imp_type(context).as("imp_type")
        ),
        List().toMap,
        List().toMap
      )
  }

  def member_id(context: Context) = {
    val spark  = context.spark
    val Config = context.config
    coalesce(
      when(
        (when(
          is_not_null(col("log_impbus_impressions_pricing.buyer_charges.is_dw"))
            .and(
              is_not_null(
                col("log_impbus_impressions_pricing.seller_charges.is_dw")
              )
            ),
          col("log_impbus_impressions_pricing.buyer_charges.is_dw").cast(
            ByteType
          ) + col("log_impbus_impressions_pricing.seller_charges.is_dw").cast(
            ByteType
          )
        ).otherwise(lit(1)) === lit(2))
          .and(col("index") === lit(1))
          .and(
            when(
              (coalesce(col("imp_type").cast(IntegerType), lit(1)) === lit(1))
                .or(
                  coalesce(col("imp_type").cast(IntegerType), lit(1)) === lit(2)
                )
                .or(
                  coalesce(col("imp_type").cast(IntegerType), lit(1)) === lit(3)
                )
                .or(
                  coalesce(col("imp_type").cast(IntegerType), lit(1)) === lit(4)
                )
                .or(
                  coalesce(col("imp_type").cast(IntegerType), lit(1)) === lit(6)
                )
                .or(
                  coalesce(col("imp_type").cast(IntegerType), lit(1)) === lit(8)
                ),
              col("seller_member_id").cast(IntegerType)
            ).otherwise(col("buyer_member_id").cast(IntegerType)) === col(
              "seller_member_id"
            ).cast(IntegerType)
          ),
        col("buyer_member_id").cast(IntegerType)
      ),
      when(
        (when(
          is_not_null(col("log_impbus_impressions_pricing.buyer_charges.is_dw"))
            .and(
              is_not_null(
                col("log_impbus_impressions_pricing.seller_charges.is_dw")
              )
            ),
          col("log_impbus_impressions_pricing.buyer_charges.is_dw").cast(
            ByteType
          ) + col("log_impbus_impressions_pricing.seller_charges.is_dw").cast(
            ByteType
          )
        ).otherwise(lit(1)) === lit(2))
          .and(col("index") === lit(1))
          .and(
            when(
              (coalesce(col("imp_type").cast(IntegerType), lit(1)) === lit(1))
                .or(
                  coalesce(col("imp_type").cast(IntegerType), lit(1)) === lit(2)
                )
                .or(
                  coalesce(col("imp_type").cast(IntegerType), lit(1)) === lit(3)
                )
                .or(
                  coalesce(col("imp_type").cast(IntegerType), lit(1)) === lit(4)
                )
                .or(
                  coalesce(col("imp_type").cast(IntegerType), lit(1)) === lit(6)
                )
                .or(
                  coalesce(col("imp_type").cast(IntegerType), lit(1)) === lit(8)
                ),
              col("seller_member_id").cast(IntegerType)
            ).otherwise(col("buyer_member_id").cast(IntegerType)) === col(
              "buyer_member_id"
            ).cast(IntegerType)
          ),
        col("seller_member_id").cast(IntegerType)
      ),
      when(
        (coalesce(col("imp_type").cast(IntegerType), lit(1)) === lit(1))
          .or(coalesce(col("imp_type").cast(IntegerType), lit(1)) === lit(2))
          .or(coalesce(col("imp_type").cast(IntegerType), lit(1)) === lit(3))
          .or(coalesce(col("imp_type").cast(IntegerType), lit(1)) === lit(4))
          .or(coalesce(col("imp_type").cast(IntegerType), lit(1)) === lit(6))
          .or(coalesce(col("imp_type").cast(IntegerType), lit(1)) === lit(8)),
        col("seller_member_id").cast(IntegerType)
      ).otherwise(col("buyer_member_id").cast(IntegerType))
    )
  }

  def imp_type(context: Context) = {
    val spark  = context.spark
    val Config = context.config
    coalesce(
      when(
        (when(
          is_not_null(col("log_impbus_impressions_pricing.buyer_charges.is_dw"))
            .and(
              is_not_null(
                col("log_impbus_impressions_pricing.seller_charges.is_dw")
              )
            ),
          col("log_impbus_impressions_pricing.buyer_charges.is_dw").cast(
            ByteType
          ) + col("log_impbus_impressions_pricing.seller_charges.is_dw").cast(
            ByteType
          )
        ).otherwise(lit(1)) === lit(2))
          .and(col("index") === lit(1))
          .and(
            when(
              (coalesce(col("imp_type").cast(IntegerType), lit(1)) === lit(1))
                .or(
                  coalesce(col("imp_type").cast(IntegerType), lit(1)) === lit(2)
                )
                .or(
                  coalesce(col("imp_type").cast(IntegerType), lit(1)) === lit(3)
                )
                .or(
                  coalesce(col("imp_type").cast(IntegerType), lit(1)) === lit(4)
                )
                .or(
                  coalesce(col("imp_type").cast(IntegerType), lit(1)) === lit(6)
                )
                .or(
                  coalesce(col("imp_type").cast(IntegerType), lit(1)) === lit(8)
                ),
              col("seller_member_id").cast(IntegerType)
            ).otherwise(col("buyer_member_id").cast(IntegerType)) === col(
              "seller_member_id"
            ).cast(IntegerType)
          )
          .and(coalesce(col("imp_type").cast(IntegerType), lit(1)) === lit(6)),
        lit(7)
      ),
      when(
        (when(
          is_not_null(col("log_impbus_impressions_pricing.buyer_charges.is_dw"))
            .and(
              is_not_null(
                col("log_impbus_impressions_pricing.seller_charges.is_dw")
              )
            ),
          col("log_impbus_impressions_pricing.buyer_charges.is_dw").cast(
            ByteType
          ) + col("log_impbus_impressions_pricing.seller_charges.is_dw").cast(
            ByteType
          )
        ).otherwise(lit(1)) === lit(2))
          .and(col("index") === lit(1))
          .and(
            when(
              (coalesce(col("imp_type").cast(IntegerType), lit(1)) === lit(1))
                .or(
                  coalesce(col("imp_type").cast(IntegerType), lit(1)) === lit(2)
                )
                .or(
                  coalesce(col("imp_type").cast(IntegerType), lit(1)) === lit(3)
                )
                .or(
                  coalesce(col("imp_type").cast(IntegerType), lit(1)) === lit(4)
                )
                .or(
                  coalesce(col("imp_type").cast(IntegerType), lit(1)) === lit(6)
                )
                .or(
                  coalesce(col("imp_type").cast(IntegerType), lit(1)) === lit(8)
                ),
              col("seller_member_id").cast(IntegerType)
            ).otherwise(col("buyer_member_id").cast(IntegerType)) === col(
              "seller_member_id"
            ).cast(IntegerType)
          )
          .and(coalesce(col("imp_type").cast(IntegerType), lit(1)) === lit(7)),
        lit(6)
      ),
      when(
        (when(
          is_not_null(col("log_impbus_impressions_pricing.buyer_charges.is_dw"))
            .and(
              is_not_null(
                col("log_impbus_impressions_pricing.seller_charges.is_dw")
              )
            ),
          col("log_impbus_impressions_pricing.buyer_charges.is_dw").cast(
            ByteType
          ) + col("log_impbus_impressions_pricing.seller_charges.is_dw").cast(
            ByteType
          )
        ).otherwise(lit(1)) === lit(2))
          .and(col("index") === lit(1))
          .and(
            when(
              (coalesce(col("imp_type").cast(IntegerType), lit(1)) === lit(1))
                .or(
                  coalesce(col("imp_type").cast(IntegerType), lit(1)) === lit(2)
                )
                .or(
                  coalesce(col("imp_type").cast(IntegerType), lit(1)) === lit(3)
                )
                .or(
                  coalesce(col("imp_type").cast(IntegerType), lit(1)) === lit(4)
                )
                .or(
                  coalesce(col("imp_type").cast(IntegerType), lit(1)) === lit(6)
                )
                .or(
                  coalesce(col("imp_type").cast(IntegerType), lit(1)) === lit(8)
                ),
              col("seller_member_id").cast(IntegerType)
            ).otherwise(col("buyer_member_id").cast(IntegerType)) === col(
              "buyer_member_id"
            ).cast(IntegerType)
          )
          .and(coalesce(col("imp_type").cast(IntegerType), lit(1)) === lit(6)),
        lit(7)
      ),
      when(
        (when(
          is_not_null(col("log_impbus_impressions_pricing.buyer_charges.is_dw"))
            .and(
              is_not_null(
                col("log_impbus_impressions_pricing.seller_charges.is_dw")
              )
            ),
          col("log_impbus_impressions_pricing.buyer_charges.is_dw").cast(
            ByteType
          ) + col("log_impbus_impressions_pricing.seller_charges.is_dw").cast(
            ByteType
          )
        ).otherwise(lit(1)) === lit(2))
          .and(col("index") === lit(1))
          .and(
            when(
              (coalesce(col("imp_type").cast(IntegerType), lit(1)) === lit(1))
                .or(
                  coalesce(col("imp_type").cast(IntegerType), lit(1)) === lit(2)
                )
                .or(
                  coalesce(col("imp_type").cast(IntegerType), lit(1)) === lit(3)
                )
                .or(
                  coalesce(col("imp_type").cast(IntegerType), lit(1)) === lit(4)
                )
                .or(
                  coalesce(col("imp_type").cast(IntegerType), lit(1)) === lit(6)
                )
                .or(
                  coalesce(col("imp_type").cast(IntegerType), lit(1)) === lit(8)
                ),
              col("seller_member_id").cast(IntegerType)
            ).otherwise(col("buyer_member_id").cast(IntegerType)) === col(
              "buyer_member_id"
            ).cast(IntegerType)
          )
          .and(coalesce(col("imp_type").cast(IntegerType), lit(1)) === lit(7)),
        lit(6)
      ),
      coalesce(col("imp_type").cast(IntegerType), lit(1))
    )
  }

}
