package uk.ac.wellcome.platform.archive.common.models.bagit

import java.nio.file.Paths

import uk.ac.wellcome.storage.ObjectLocation

case class BagItemLocation(
  bagLocation: BagLocation,
  bagItemPath: BagItemPath
) {
  def completePath: String =
    Paths.get(bagLocation.completePath, bagItemPath.toString).toString

  def objectLocation: ObjectLocation =
    ObjectLocation(
      namespace = bagLocation.storageNamespace,
      key = completePath
    )
}
