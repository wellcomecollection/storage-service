package uk.ac.wellcome.platform.archive.bagverifier.storage.bag

import uk.ac.wellcome.platform.archive.bagverifier.storage.{
  Locatable,
  LocateFailure,
  LocationNotFound
}
import uk.ac.wellcome.platform.archive.common.bagit.models.BagPath
import uk.ac.wellcome.storage.{ObjectLocation, ObjectLocationPrefix}

object BagLocatable {
  implicit val bagPathLocatable
    : Locatable[ObjectLocation, ObjectLocationPrefix, BagPath] =
    new Locatable[ObjectLocation, ObjectLocationPrefix, BagPath] {
      override def locate(bagPath: BagPath)(
        maybeRoot: Option[ObjectLocationPrefix]
      ): Either[LocateFailure[BagPath], ObjectLocation] =
        maybeRoot match {
          case None       => Left(LocationNotFound(bagPath, s"No root specified!"))
          case Some(root) => Right(root.asLocation(bagPath.value))
        }
    }
}
