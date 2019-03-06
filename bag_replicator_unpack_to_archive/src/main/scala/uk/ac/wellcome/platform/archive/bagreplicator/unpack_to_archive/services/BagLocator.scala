package uk.ac.wellcome.platform.archive.bagreplicator.unpack_to_archive.services

import com.amazonaws.services.s3.AmazonS3
import uk.ac.wellcome.platform.archive.common.ConvertibleToInputStream._
import uk.ac.wellcome.platform.archive.common.bagit.{BagInfoLocator, S3BagFile}
import uk.ac.wellcome.platform.archive.common.parsers.BagInfoParser
import uk.ac.wellcome.storage.ObjectLocation

import scala.concurrent.{ExecutionContext, Future}

class BagLocator(s3Client: AmazonS3)(implicit ec: ExecutionContext) {
  val s3BagFile = new S3BagFile(s3Client)

  implicit val _ = s3Client

  def getBagIdentifier(objectLocation: ObjectLocation) =
    for {
      bagInfoPath <- locateBagInfo(objectLocation)

      inputStream <- objectLocation
        .copy(key = bagInfoPath)
        .toInputStream

      bagInfo <- BagInfoParser.create(inputStream)

    } yield bagInfo.externalIdentifier

  def getBagRoot(location: ObjectLocation) = {
    locateBagInfo(location)
      .map(BagInfoLocator.bagPathFrom)
      .map(bagRoot => location.copy(key = bagRoot))
  }

  private def locateBagInfo(location: ObjectLocation) =
    Future.fromTry(
      s3BagFile.locateBagInfo(location)
    )
}
