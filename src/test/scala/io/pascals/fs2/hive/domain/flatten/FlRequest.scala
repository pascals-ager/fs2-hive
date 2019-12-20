package io.pascals.fs2.hive.domain.flatten

import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}
import io.circe.{Decoder, Encoder}

case class FlRequest(url: String,
                     headers: Map[String, String],
                     parameters: Map[String, List[String]],
                     remote_address: String)

object FlRequest {

  implicit val OwRequestDecoder: Decoder[FlRequest] = deriveDecoder[FlRequest]
  implicit val OwRequestEncoder: Encoder[FlRequest] = deriveEncoder[FlRequest]
}
