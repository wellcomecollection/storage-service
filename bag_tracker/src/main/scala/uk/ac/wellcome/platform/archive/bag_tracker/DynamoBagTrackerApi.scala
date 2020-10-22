package uk.ac.wellcome.platform.archive.bag_tracker

import akka.actor.ActorSystem
import akka.http.scaladsl.model.headers.Location
import akka.http.scaladsl.model.{HttpResponse, StatusCodes}
import akka.http.scaladsl.server.Directives.complete
import akka.http.scaladsl.server.Route
import uk.ac.wellcome.platform.archive.bag_tracker.storage.dynamo.DynamoStorageManifestDao
import uk.ac.wellcome.platform.archive.common.bagit.models.{BagId, BagVersion}
import uk.ac.wellcome.storage._
import uk.ac.wellcome.storage.services.s3.S3Uploader

import scala.concurrent.duration._

class DynamoBagTrackerApi(
  dynamoStorageManifestDao: DynamoStorageManifestDao,
  s3Uploader: S3Uploader
)(host: String, port: Int)(implicit actorSystem: ActorSystem)
    extends BagTrackerApi(dynamoStorageManifestDao)(host, port) {

  private def getPresignedUrl(
    bagId: BagId,
    version: Int
  ): Either[ReadError, Route] =
    dynamoStorageManifestDao.indexedStore.get(Version(bagId, version)) match {
      case Right(Identified(_, location)) =>
        info(s"Found bag id=$bagId version=$version")
        s3Uploader.getPresignedGetURL(location, expiryLength = 1.hour).map {
          url =>
            complete(
              HttpResponse(
                status = StatusCodes.TemporaryRedirect,
                headers = Location(url.toExternalForm) :: Nil
              )
            )
        }

      case Left(err) => Left(err)
    }

  override def getBag(bagId: BagId, version: BagVersion): Route =
    getPresignedUrl(bagId, version.underlying) match {
      case Right(redirect) => redirect
      case Left(err)       => handleGetErrors(bagId, version, err)
    }

  override def getLatestBag(bagId: BagId): Route = {
    val redirectRoute =
      dynamoStorageManifestDao.indexedStore
        .max(bagId)
        .flatMap { version =>
          getPresignedUrl(bagId, version)
        }

    redirectRoute match {
      case Right(redirect) => redirect
      case Left(err)       => handleGetLatestBagErrors(bagId, err)
    }
  }
}
