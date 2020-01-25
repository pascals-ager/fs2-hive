package io.pascals.fs2.hive.domain.flatten

import java.time.OffsetDateTime
import java.time.format.DateTimeFormatter

import cats.Id
import io.circe.Decoder.Result
import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}
import io.circe.{Decoder, Encoder, HCursor, Json}

case class FlClicks[F[_], G[_]](
    id: F[String],
    ts: OffsetDateTime,
    isp: F[String],
    service_id: F[Int],
    campaign_id: F[String],
    traffic_id: F[String],
    external_id: F[String],
    target_url: F[String],
    processing_result: F[FlProcessingResult[Option]],
    request: G[FlRequest],
    device: F[Map[String, String]],
    country_iso_name: F[String],
    city: F[String]
)

object FlClicks {

  implicit val OffsetDateTimeFormat
      : Encoder[OffsetDateTime] with Decoder[OffsetDateTime] =
    new Encoder[OffsetDateTime] with Decoder[OffsetDateTime] {
      override def apply(a: OffsetDateTime): Json =
        Encoder.encodeString.apply(
          a.format(DateTimeFormatter.ISO_OFFSET_DATE_TIME)
        )

      override def apply(c: HCursor): Result[OffsetDateTime] =
        Decoder.decodeString
          .map(s =>
            OffsetDateTime.parse(s, DateTimeFormatter.ISO_OFFSET_DATE_TIME)
          )
          .apply(c)
    }
  implicit val OwClicksDecoder: Decoder[FlClicks[Option, Id]] =
    deriveDecoder[FlClicks[Option, Id]]
  implicit val OwClicksEncoder: Encoder[FlClicks[Option, Id]] =
    deriveEncoder[FlClicks[Option, Id]]
}
