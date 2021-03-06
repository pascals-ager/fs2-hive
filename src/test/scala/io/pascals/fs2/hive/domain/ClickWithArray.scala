package io.pascals.fs2.hive.domain

import java.sql.{Timestamp => SqlTimestamp}

import io.circe.Decoder.Result
import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}
import io.circe.{Decoder, Encoder, HCursor, Json}

case class ClickWithArray(
    `type`: String,
    id: String,
    referenced_event_id: Option[String],
    happened: SqlTimestamp,
    processed: SqlTimestamp,
    tracking_id: String,
    source_attributes: SourceAttributesWithArray,
    event_data_strategies: Option[Strategy],
    year: Int,
    month: Int,
    day: Int
)

object ClickWithArray {

  implicit val SqlTimestampFormat
      : Encoder[SqlTimestamp] with Decoder[SqlTimestamp] =
    new Encoder[SqlTimestamp] with Decoder[SqlTimestamp] {
      override def apply(a: SqlTimestamp): Json =
        Encoder.encodeString.apply(a.toString)

      override def apply(c: HCursor): Result[SqlTimestamp] =
        Decoder.decodeString.map(s => SqlTimestamp.valueOf(s)).apply(c)
    }

  implicit val ClickWithArrayDecoder: Decoder[ClickWithArray] =
    deriveDecoder[ClickWithArray]
  implicit val ClickWithArrayEncoder: Encoder[ClickWithArray] =
    deriveEncoder[ClickWithArray]
}
