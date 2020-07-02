package uk.ac.wellcome.platform.archive.bagverifier.storage.bag

import uk.ac.wellcome.platform.archive.bagverifier.storage.{
  Locatable,
  LocateFailure,
  LocationNotFound
}
import uk.ac.wellcome.platform.archive.common.bagit.models.BagPath
import uk.ac.wellcome.storage.{Location, Prefix}

object BagLocatable {
  implicit def bagPathLocatable[BagLocation <: Location, BagPrefix <: Prefix[BagLocation]]
    : Locatable[BagLocation, BagPrefix, BagPath] =
    new Locatable[BagLocation, BagPrefix, BagPath] {
      override def locate(bagPath: BagPath)(
        maybeRoot: Option[BagPrefix]
      ): Either[LocateFailure[BagPath], BagLocation] =
        maybeRoot match {
          case None       => Left(LocationNotFound(bagPath, s"No root specified!"))
          case Some(root) => Right(root.asLocation(bagPath.value))
        }
    }
}
