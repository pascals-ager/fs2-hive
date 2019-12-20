package io.pascals.fs2.hive.domain.flatten

import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}
import io.circe.{Decoder, Encoder}

case class FlHeaders(sec_fetch_mode: String,
                     sec_fetch_site: String,
                     accept_language: String,
                     x_forwarded_proto: String,
                     x_forwarded_port: String,
                     x_forwarded_for: String,
                     sec_fetch_user: String,
                     accept: String,
                     x_real_ip: String,
                     x_forwarded_server: String,
                     x_forwarded_host: String,
                     host: String,
                     upgrade_insecure_requests: String,
                     x_requested_with: String,
                     accept_encoding: String,
                     user_agent: String)


object FlHeaders {

  implicit val OwHeadersDecoder: Decoder[FlHeaders] = deriveDecoder[FlHeaders]
  implicit val OwHeadersEncoder: Encoder[FlHeaders] = deriveEncoder[FlHeaders]
}
