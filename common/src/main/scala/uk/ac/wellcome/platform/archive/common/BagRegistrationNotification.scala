package uk.ac.wellcome.platform.archive.common

import io.circe.generic.extras.JsonKey
import uk.ac.wellcome.platform.archive.common.bagit.models.{BagVersion, ExternalIdentifier}
import uk.ac.wellcome.platform.archive.common.storage.models.StorageSpace

/** This notification is sent by the storage service to notify another system
  * (which may be entirely separate from the storage service, e.g. the catalogue)
  * that a bag has been registered.
  *
  */
case class BagRegistrationNotification(
  space: StorageSpace,
  externalIdentifier: ExternalIdentifier,
  version: BagVersion,
  @JsonKey("type") ontologyType: String = "RegisteredBagNotification"
)
