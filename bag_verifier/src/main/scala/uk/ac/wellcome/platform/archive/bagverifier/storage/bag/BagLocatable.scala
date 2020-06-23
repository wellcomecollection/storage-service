package uk.ac.wellcome.platform.archive.bagverifier.storage.bag

import uk.ac.wellcome.platform.archive.bagverifier.storage.{
  Locatable,
  LocateFailure,
  LocationNotFound
}
import uk.ac.wellcome.platform.archive.common.bagit.models.BagPath
import uk.ac.wellcome.storage.ObjectLocation

object BagLocatable {
  implicit val bagPathLocatable: Locatable[BagPath] = new Locatable[BagPath]  {
    override def locate(bagPath: BagPath)(
      maybeRoot: Option[ObjectLocation]
    ): Either[LocateFailure[BagPath], ObjectLocation] =
      maybeRoot match {
        case None       => Left(LocationNotFound(bagPath, s"No root specified!"))
        case Some(root) => Right(root.join(bagPath.value))
      }
  }
}
