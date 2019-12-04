package uk.ac.wellcome.platform.archive.common.bagit.models

import org.scanamo.DynamoFormat
import uk.ac.wellcome.platform.archive.common.storage.models.StorageSpace

case class BagId(
  space: StorageSpace,
  externalIdentifier: ExternalIdentifier
) {
  override def toString: String =
    s"$space/$externalIdentifier"
}

case object BagId {
  // We can parse a bag ID this way because storage spaces never contain
  // slashes, only the external identifier.
  //
  // The first slash can be treated as part of the bag ID, and any remaining
  // slashes can be treated as part of the external identifier.
  def apply(value: String): BagId =
    value.split("/", 2) match {
      case Array(space, externalIdentifier) =>
        BagId(
          space = StorageSpace(space),
          externalIdentifier = ExternalIdentifier(externalIdentifier)
        )

      case _ =>
        throw new IllegalArgumentException(s"Cannot create bag ID from $value")
    }

  implicit def evidence: DynamoFormat[BagId] =
    DynamoFormat.coercedXmap[BagId, String, IllegalArgumentException](
      BagId(_)
    )(
      _.toString
    )
}
