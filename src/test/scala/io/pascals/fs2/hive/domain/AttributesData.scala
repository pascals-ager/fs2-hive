package io.pascals.fs2.hive.domain

import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}
import io.circe.{Decoder, Encoder}

case class AttributesData( remote_address: Option[String],
                           url: Option[String],
                           headers: Option[Map[String, String]],
                           parameters: Option[Map[String, String]])

object AttributesData {

  implicit val AttributesDataDecoder: Decoder[AttributesData] = deriveDecoder[AttributesData]
  implicit val AttributesDataEncoder: Encoder[AttributesData] = deriveEncoder[AttributesData]
}
