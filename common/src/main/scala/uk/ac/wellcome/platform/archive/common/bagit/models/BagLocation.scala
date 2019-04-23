package uk.ac.wellcome.platform.archive.common.bagit.models

import java.nio.file.Paths

import uk.ac.wellcome.platform.archive.common.storage.models.StorageSpace
import uk.ac.wellcome.storage.ObjectLocation

/** Represents the complete location of a Bag in S3.
  *
  * @param storageNamespace The name of the S3 bucket
  * @param storagePrefix The global prefix for all objects in storage
  *                      (e.g. "archive")
  * @param storageSpace The namespace from the ingest request (e.g. "digitised")
  * @param bagPath The relative path to the bag (e.g. "b12345.zip")
  */
case class BagLocation(
  storageNamespace: String,
  storagePrefix: Option[String],
  storageSpace: StorageSpace,
  bagPath: BagPath
) {
  def completePath: String =
    Paths
      .get(
        storagePrefix.getOrElse(""),
        storageSpace.toString,
        bagPath.toString
      )
      .toString

  def objectLocation: ObjectLocation =
    ObjectLocation(
      namespace = storageNamespace,
      key = completePath
    )
}
