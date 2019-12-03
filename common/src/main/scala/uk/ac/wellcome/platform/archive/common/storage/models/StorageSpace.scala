package uk.ac.wellcome.platform.archive.common.storage.models

import org.scanamo.DynamoFormat
import io.circe.{Decoder, Encoder, HCursor, Json}

case class StorageSpace(underlying: String) {
  override def toString: String = underlying

  require(!underlying.isEmpty, "Storage space cannot be empty")

  // At various points in the pipeline, we combine the storage space and
  // the external identifier into a bag ID, for example:
  //
  //    space = "digitised"
  //    identifier = "b12345678"
  //     => bag ID = "digitised/b12345678"
  //
  // We allow slashes in the IDs (for example, to match CALM IDs like PP/MIA/1).
  // To avoid ambiguity when looking at a bag ID, we forbid slashes in
  // the external identifier.
  //
  // For example, this allows us to say unambiguously that
  //
  //    bag ID = "alfa/bravo/charlie"
  //     => space = "alfa"
  //        identifier = "bravo/charlie"
  //
  // If both the space and external identifier could contain slashes,
  // this bag ID would be ambiguous.
  require(
    !underlying.contains("/"),
    s"Storage space cannot contain slashes, but got $underlying"
  )
}

object StorageSpace {
  implicit val encoder: Encoder[StorageSpace] = (value: StorageSpace) =>
    Json.fromString(value.toString)

  implicit val decoder: Decoder[StorageSpace] = (cursor: HCursor) =>
    cursor.value.as[String].map(StorageSpace(_))

  implicit def evidence: DynamoFormat[StorageSpace] =
    DynamoFormat.iso[StorageSpace, String](
      StorageSpace(_)
    )(
      _.underlying
    )
}
