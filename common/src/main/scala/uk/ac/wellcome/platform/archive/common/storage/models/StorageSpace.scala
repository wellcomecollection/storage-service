package uk.ac.wellcome.platform.archive.common.storage.models

import com.gu.scanamo.DynamoFormat
import io.circe.{Decoder, Encoder, Json}

case class StorageSpace(underlying: String) extends AnyVal {
  override def toString: String = underlying
}

object StorageSpace {
  implicit val encoder: Encoder[StorageSpace] = Encoder.instance[StorageSpace] {
    space: StorageSpace =>
      Json.fromString(space.toString)
  }

  implicit val decoder: Decoder[StorageSpace] = Decoder.instance[StorageSpace](cursor =>
    cursor.value.as[String].map(StorageSpace(_)))

  implicit def evidence: DynamoFormat[StorageSpace] =
    DynamoFormat.coercedXmap[StorageSpace, String, IllegalArgumentException](
      StorageSpace(_)
    )(
      _.underlying
    )
}
