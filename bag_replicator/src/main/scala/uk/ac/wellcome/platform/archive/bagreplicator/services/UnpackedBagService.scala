package uk.ac.wellcome.platform.archive.bagreplicator.services

import java.io.InputStream

import com.amazonaws.services.s3.AmazonS3
import uk.ac.wellcome.platform.archive.common.ConvertibleToInputStream._
import uk.ac.wellcome.platform.archive.common.bagit.{BagInfoLocator, S3BagFile}
import uk.ac.wellcome.platform.archive.common.models.bagit.{
  BagInfo,
  ExternalIdentifier
}
import uk.ac.wellcome.platform.archive.common.parsers.BagInfoParser
import uk.ac.wellcome.storage.ObjectLocation

import scala.concurrent.{ExecutionContext, Future}

class UnpackedBagService(s3Client: AmazonS3)(implicit ec: ExecutionContext) {
  val s3BagFile = new S3BagFile(s3Client)

  implicit val _ = s3Client

  def getBagIdentifier(
    objectLocation: ObjectLocation): Future[ExternalIdentifier] =
    for {
      bagInfoPath: String <- Future.fromTry(
        s3BagFile.locateBagInfo(objectLocation))
      inputStream: InputStream <- objectLocation
        .copy(key = bagInfoPath)
        .toInputStream
      bagInfo: BagInfo <- BagInfoParser.create(inputStream)
    } yield bagInfo.externalIdentifier

  def getBagRoot(objectLocation: ObjectLocation): Future[ObjectLocation] =
    for {
      bagInfoPath: String <- Future.fromTry(
        s3BagFile.locateBagInfo(objectLocation))
      bagRoot = BagInfoLocator.bagPathFrom(bagInfoPath)
    } yield objectLocation.copy(key = bagRoot)
}
