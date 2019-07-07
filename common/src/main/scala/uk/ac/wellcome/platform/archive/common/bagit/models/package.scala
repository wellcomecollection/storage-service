package uk.ac.wellcome.platform.archive.common.bagit

import uk.ac.wellcome.platform.archive.common.storage.{
  Locatable,
  LocateFailure,
  LocationNotFound
}
import uk.ac.wellcome.storage.ObjectLocation

package object models {
  private def locateBagPath(root: ObjectLocation)(
    bagPath: BagPath): ObjectLocation =
    root.join(bagPath.value)

  implicit val bagPathLocator: Locatable[BagPath] = new Locatable[BagPath] {
    override def locate(bagPath: BagPath)(maybeRoot: Option[ObjectLocation])
      : Either[LocateFailure[BagPath], ObjectLocation] =
      maybeRoot match {
        case None => Left(LocationNotFound(bagPath, s"No root specified!"))
        case Some(root) => Right(locateBagPath(root)(bagPath))
      }
  }

  implicit val bagFileLocator: Locatable[BagFile] = new Locatable[BagFile] {
    override def locate(bagFile: BagFile)(maybeRoot: Option[ObjectLocation])
      : Either[LocateFailure[BagFile], ObjectLocation] =
      maybeRoot match {
        case None => Left(LocationNotFound(bagFile, s"No root specified!"))
        case Some(root) => Right(locateBagPath(root)(bagFile.path))
      }
  }
}
