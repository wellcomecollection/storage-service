package uk.ac.wellcome.platform.archive.common.verify

import grizzled.slf4j.Logging
import io.circe.{Decoder, Encoder, HCursor, Json}
import org.scanamo.DynamoFormat

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
      ChecksumValue(_),
      _.toString
    )
}
