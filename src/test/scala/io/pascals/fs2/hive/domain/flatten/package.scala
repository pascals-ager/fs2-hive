package io.pascals.fs2.hive.domain

import java.sql.{Timestamp => SqlTimestamp}
import java.time.{Instant, LocalDateTime, OffsetDateTime, ZoneOffset}

import cats.Id
import cats.effect.IO
import fs2.kafka.{AutoOffsetReset, ConsumerSettings, Deserializer, IsolationLevel}
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hive.conf.HiveConf
import io.pascals.fs2.hive.domain.kafka.KafkaMetadata
import io.pascals.fs2.hive.utils.Transform

package object flatten {

  implicit def OwEnrich: Transform[IO, (KafkaMetadata[Option], FlClicks[Option, Id]), FlClicksKafkaEnriched[Option]] = in => {
    FlClicksKafkaEnriched(
      in._2,
      Some(in._1)
    )
  }

  implicit def FlFlatten: Transform[IO, FlClicksKafkaEnriched[Option], FlClicksFlattened[Option]] = in => {
    val happened_ts: OffsetDateTime = in.click.ts
    val processed_ts: OffsetDateTime = in.click.ts
    val happened_utc: LocalDateTime = happened_ts.atZoneSameInstant(ZoneOffset.UTC).toLocalDateTime
    lazy val happened_sql_ts: SqlTimestamp = SqlTimestamp.valueOf(happened_utc)
    val processed_utc: LocalDateTime = processed_ts.atZoneSameInstant(ZoneOffset.UTC).toLocalDateTime
    lazy val processed_sql_ts: SqlTimestamp = SqlTimestamp.valueOf(processed_utc)
    var score: Option[Int] = None
    var fingerprint: Option[String] = None
    var token: Option[String] = None
    var redirect_url: Option[String] = None
    var strategies: Option[List[FlStrategy]] = None
    var decision: Option[String] = None
    var offset: Option[Long] = None
    var partition: Option[Int] = None
    var topic: Option[String] = None
    var kafka_sql_ts: Option[SqlTimestamp] = None
    in.meta match {
      case Some(meta) =>
        meta.ts match {
          case Some(ts) =>
            val kafka_ts: OffsetDateTime = OffsetDateTime.ofInstant(Instant.ofEpochMilli(ts.createTime.getOrElse(0L)), ZoneOffset.UTC)
            val kafka_utc: LocalDateTime = kafka_ts.atZoneSameInstant(ZoneOffset.UTC).toLocalDateTime
            kafka_sql_ts = Some(SqlTimestamp.valueOf(kafka_utc))
          case None =>
        }
        offset = meta.offset
        partition = meta.partition
        topic = meta.topic

      case None =>
    }
    in.click.processing_result match {
      case Some(res) =>
        score = res.score
        fingerprint = res.fingerprint
        token = res.token
        redirect_url = res.redirect_url
        strategies = res.strategies
        decision = res.decision
      case None =>
    }
    FlClicksFlattened[Option](
      Some("Flatten"),
      in.click.id,
      in.click.isp,
      in.click.service_id,
      in.click.campaign_id,
      in.click.traffic_id,
      in.click.external_id,
      in.click.target_url,
      score,
      fingerprint,
      redirect_url,
      token,
      strategies,
      decision,
      Some(in.click.request),
      in.click.device,
      in.click.country_iso_name,
      in.click.city,
      happened_sql_ts,
      processed_sql_ts,
      offset,
      partition,
      topic,
      kafka_sql_ts,
      happened_utc.getYear,
      happened_utc.getMonthValue,
      happened_utc.getDayOfMonth)
  }

  implicit val consumerSettings: ConsumerSettings[IO, Option[String], String] =
    ConsumerSettings(
      keyDeserializer = Deserializer[IO, Option[String]],
      valueDeserializer = Deserializer[IO, String]
    ).withAutoOffsetReset(AutoOffsetReset.Earliest)
      .withBootstrapServers("kafka-zk:9092")
      .withGroupId("fs2-flatten.clicks-demo")
      .withIsolationLevel(IsolationLevel.ReadCommitted)

  implicit val subscribeTopic: String = "flatten-clicks"

  val hiveConfPath = "src/main/resources/hive-site.xml"
  implicit val hiveConf: HiveConf = new HiveConf()
  hiveConf.addResource(new Path(hiveConfPath))

  implicit val dbName: String = "test_db"
  implicit val tblName: String = "kafka_clicks"
}

