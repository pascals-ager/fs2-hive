package io.pascals.fs2.hive.utils

import fs2.Chunk

trait HiveStreamingSink[IO] {
  def writeSink(in: Chunk[Byte]) : cats.effect.IO[Unit]
}

/*

//  And eventually the call itself:
val stream: Stream[IO, String] = Stream(
  "17,Fs2StreamBinding,testContinent,testCountry",
  "18,Fs2StreamBinding,testContinent,testCountry",
  "19,Fs2StreamBinding,testContinent,testCountry",
  "20,Fs2StreamBinding,testContinent,testCountry"
).covary[IO]

Stream.resource(HiveStreamingSink.create[IO]).flatMap { hive =>
  stream
    .map(s => Chunk.bytes(s.getBytes))
    .evalMap(hive.write(bytes))
    .adaptError { case e => new YourException(s"My exception occurred. ${e}") }
}
*/