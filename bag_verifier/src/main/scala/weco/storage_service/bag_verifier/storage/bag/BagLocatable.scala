package weco.storage_service.bag_verifier.storage.bag

import weco.storage_service.bag_verifier.storage.{
  Locatable,
  LocateFailure,
  LocationNotFound
}
import weco.storage_service.bagit.models.BagPath
import weco.storage.{Location, Prefix}

object BagLocatable {
  implicit def bagPathLocatable[BagLocation <: Location, BagPrefix <: Prefix[
    BagLocation
  ]]: Locatable[BagLocation, BagPrefix, BagPath] =
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
