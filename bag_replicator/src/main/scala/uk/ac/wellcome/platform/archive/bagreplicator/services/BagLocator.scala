package uk.ac.wellcome.platform.archive.bagreplicator.services

import java.io.InputStream

import com.amazonaws.services.s3.AmazonS3
import uk.ac.wellcome.platform.archive.common.ConvertibleToInputStream._
import uk.ac.wellcome.platform.archive.common.bagit.models.{
  BagInfo,
  ExternalIdentifier
}
import uk.ac.wellcome.platform.archive.common.bagit.parsers.BagInfoParser
import uk.ac.wellcome.storage.ObjectLocation

import scala.concurrent.{ExecutionContext, Future}

class BagLocator(s3Client: AmazonS3)(implicit ec: ExecutionContext) {
  val s3BagLocator = new S3BagLocator(s3Client)

  implicit val _ = s3Client

  def getBagIdentifier(
    objectLocation: ObjectLocation): Future[ExternalIdentifier] =
    for {
      bagInfoLocation <- Future.fromTry {
        s3BagLocator.locateBagInfo(objectLocation)
      }
      inputStream: InputStream <- bagInfoLocation
        .toInputStream
      bagInfo: BagInfo <- BagInfoParser.create(inputStream)
    } yield bagInfo.externalIdentifier
}
