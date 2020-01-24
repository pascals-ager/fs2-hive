package io.pascals.fs2.hive.domain

import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}
import io.circe.{Decoder, Encoder}

case class AttributesDataWithArray(
    remote_address: Option[String],
    url: Option[String],
    headers: Option[Map[String, String]],
    parameters: Option[Map[String, List[String]]]
)

object AttributesDataWithArray {

  implicit val AttributesDataWithArrayDecoder
      : Decoder[AttributesDataWithArray] =
    deriveDecoder[AttributesDataWithArray]
  implicit val AttributesDataWithArrayEncoder
      : Encoder[AttributesDataWithArray] =
    deriveEncoder[AttributesDataWithArray]
}
