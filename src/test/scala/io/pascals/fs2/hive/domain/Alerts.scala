package io.pascals.fs2.hive.domain

import java.sql.{Timestamp => SqlTimestamp}

import io.circe.Decoder.Result
import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}
import io.circe.{Decoder, Encoder, HCursor, Json}

case class Alerts (id: Int,
                   tracking_id: String,
                   msg: String,
                   continent: String,
                   country: String,
                   event_time: SqlTimestamp,
                   year: Int,
                   month: Int,
                   day: Int)

object Alerts {

  implicit val SqlTimestampFormat : Encoder[SqlTimestamp] with Decoder[SqlTimestamp] = new Encoder[SqlTimestamp] with Decoder[SqlTimestamp] {
    override def apply(a: SqlTimestamp): Json = Encoder.encodeString.apply(a.toString)

    override def apply(c: HCursor): Result[SqlTimestamp] = Decoder.decodeString.map(s => SqlTimestamp.valueOf(s) ).apply(c)
  }

  implicit val alertsDecoder: Decoder[Alerts] = deriveDecoder[Alerts]
  implicit val alertsEncoder: Encoder[Alerts] = deriveEncoder[Alerts]
}