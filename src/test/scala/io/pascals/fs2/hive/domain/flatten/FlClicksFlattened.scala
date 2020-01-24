package io.pascals.fs2.hive.domain.flatten

import java.sql.{Timestamp => SqlTimestamp}

import io.circe.Decoder.Result
import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}
import io.circe.{Decoder, Encoder, HCursor, Json}

case class FlClicksFlattened[F[_]](
    `type`: F[String],
    id: F[String],
    isp: F[String],
    service_id: F[Int],
    campaign_id: F[String],
    traffic_id: F[String],
    external_id: F[String],
    target_url: F[String],
    processing_score: F[Int],
    processing_fingerprint: F[String],
    processing_redirect_url: F[String],
    processing_token: F[String],
    processing_strategies: F[List[FlStrategy]],
    processing_decision: F[String],
    request: F[FlRequest],
    device: F[Map[String, String]],
    country_iso_name: F[String],
    city: F[String],
    happened: SqlTimestamp,
    processed: SqlTimestamp,
    kafka_offset: F[Long],
    kafka_partition: F[Int],
    kafka_topic: F[String],
    kafka_sql_ts: F[SqlTimestamp],
    year: Int,
    month: Int,
    day: Int
)

object FlClicksFlattened {

  implicit val SqlTimestampFormat
      : Encoder[SqlTimestamp] with Decoder[SqlTimestamp] =
    new Encoder[SqlTimestamp] with Decoder[SqlTimestamp] {
      override def apply(a: SqlTimestamp): Json =
        Encoder.encodeString.apply(a.toString)

      override def apply(c: HCursor): Result[SqlTimestamp] =
        Decoder.decodeString.map(s => SqlTimestamp.valueOf(s)).apply(c)
    }
  implicit val OwClicksFlattenedDecoder: Decoder[FlClicksFlattened[Option]] =
    deriveDecoder[FlClicksFlattened[Option]]
  implicit val OwClicksFlattenedEncoder: Encoder[FlClicksFlattened[Option]] =
    deriveEncoder[FlClicksFlattened[Option]]
}
