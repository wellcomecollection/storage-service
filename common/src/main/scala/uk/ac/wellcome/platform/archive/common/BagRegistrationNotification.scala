package uk.ac.wellcome.platform.archive.common

import io.circe.generic.extras.JsonKey
import uk.ac.wellcome.platform.archive.common.bagit.models.{BagVersion, ExternalIdentifier}
import uk.ac.wellcome.platform.archive.common.storage.models.StorageSpace

/** This notification is sent by the storage service to notify another system
  * (which may be entirely separate from the storage service, e.g. the catalogue)
  * that a bag has been registered.  You should be able to use this information to
  * get a bag from the external API.
  *
  * We encode a version as a string (v1, v2, v3) rather than a number because that's
  * what the external API expects.
  *
  */
case class BagRegistrationNotification(
  space: StorageSpace,
  externalIdentifier: ExternalIdentifier,
  version: String,
  @JsonKey("type") ontologyType: String = "RegisteredBagNotification"
)

case object BagRegistrationNotification {
  def apply(space: StorageSpace, externalIdentifier: ExternalIdentifier, version: BagVersion): BagRegistrationNotification =
    BagRegistrationNotification(
      space = space,
      externalIdentifier = externalIdentifier,
      version = version.toString
    )
}
