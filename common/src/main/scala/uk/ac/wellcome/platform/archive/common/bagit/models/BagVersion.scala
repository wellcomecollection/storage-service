package uk.ac.wellcome.platform.archive.common.bagit.models

import io.circe.{Decoder, Encoder, HCursor, Json}

case class BagVersion(underlying: Int) extends AnyVal {
  override def toString: String = s"v$underlying"

  def increment: BagVersion =
    BagVersion(underlying + 1)
}

object BagVersion {
  implicit val encoder: Encoder[BagVersion] =
    (version: BagVersion) => Json.fromInt(version.underlying)

  implicit val decoder: Decoder[BagVersion] = (cursor: HCursor) =>
    cursor.value.as[Int].map { BagVersion(_) }
}

