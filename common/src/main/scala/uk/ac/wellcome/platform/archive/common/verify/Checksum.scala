package uk.ac.wellcome.platform.archive.common.verify

import java.io.InputStream

import org.scanamo.DynamoFormat
import grizzled.slf4j.Logging
import io.circe.{Decoder, Encoder, HCursor, Json}

import scala.util.Try

case class Checksum(
  algorithm: HashingAlgorithm,
  value: ChecksumValue
) {
  override def toString: String =
    s"${algorithm.pathRepr}:$value"
}

case object Checksum extends Logging {
  def create(
    inputStream: InputStream,
    algorithm: HashingAlgorithm
  ): Try[Checksum] = {
    debug(s"Creating Checksum for $inputStream with  $algorithm")
    val checksum = Hasher
      .hash(inputStream)
      .map { _.getChecksumValue(algorithm) }
      .map { Checksum(algorithm, _) }

    debug(s"Got: $checksum")
    checksum
  }
}

case class ChecksumValue(value: String) {
  override def toString: String = value
}

object ChecksumValue extends Logging {
  def create(raw: String): ChecksumValue =
    ChecksumValue(raw.trim)

  implicit val encoder: Encoder[ChecksumValue] = (value: ChecksumValue) =>
    Json.fromString(value.toString)

  implicit val decoder: Decoder[ChecksumValue] = (cursor: HCursor) =>
    cursor.value.as[String].map(ChecksumValue(_))

  implicit def format: DynamoFormat[ChecksumValue] =
    DynamoFormat.iso[ChecksumValue, String](
      ChecksumValue(_)
    )(
      _.toString
    )
}

sealed trait FailedChecksum
case class FailedChecksumCreation(algorithm: HashingAlgorithm, e: Throwable)
    extends Throwable(s"Could not create checksum: ${e.getMessage}")
    with FailedChecksum
case class FailedChecksumNoMatch(actual: Checksum, expected: Checksum)
    extends Throwable(
      s"Checksum values do not match! Expected: $expected, saw: $actual"
    )
    with FailedChecksum
case class FailedChecksumLocationNotFound[T](location: VerifiableLocation)
    extends Throwable("VerifiableLocation not found!")
    with FailedChecksum
