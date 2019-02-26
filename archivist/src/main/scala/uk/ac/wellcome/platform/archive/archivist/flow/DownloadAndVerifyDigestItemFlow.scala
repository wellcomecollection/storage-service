package uk.ac.wellcome.platform.archive.archivist.flow

import java.io.InputStream

import akka.NotUsed
import akka.stream.ActorAttributes
import akka.stream.scaladsl.{Flow, Source, StreamConverters}
import com.amazonaws.services.s3.AmazonS3
import grizzled.slf4j.Logging
import uk.ac.wellcome.platform.archive.archivist.models.DigestItemJob
import uk.ac.wellcome.platform.archive.archivist.models.errors.ChecksumNotMatchedOnDownloadError
import uk.ac.wellcome.platform.archive.common.models.error.{ArchiveError, DownloadError}
import uk.ac.wellcome.storage.ObjectLocation

import scala.util.{Failure, Success, Try}

object DownloadAndVerifyDigestItemFlow extends Logging {

  def apply(parallelism: Int)(implicit s3Client: AmazonS3)
    : Flow[DigestItemJob,
           Either[ArchiveError[DigestItemJob], DigestItemJob],
           NotUsed] = {
    Flow[DigestItemJob]
      .log("download to verify")
      .flatMapMerge(
        parallelism, { job =>
          toInputStream(job.uploadLocation) match {
            case Failure(exception) =>
              warn(
                s"Failed downloading object ${job.uploadLocation} from S3 : ${exception.getMessage}")
              Source.single(
                Left(DownloadError(exception, job.uploadLocation, job)))
            case Success(inputStream) =>
              StreamConverters
                .fromInputStream(() => inputStream)
                .via(SHA256Flow())
                .map {
                  case calculatedChecksum if job.digest == calculatedChecksum =>
                    Right(job)
                  case calculatedChecksum =>
                    warn(s"Failed checksum validation in download for job $job")
                    Left(
                      ChecksumNotMatchedOnDownloadError(
                        expectedChecksum = job.digest,
                        actualChecksum = calculatedChecksum,
                        t = job
                      )
                    )
                }
          }
        }
      )
      .withAttributes(ActorAttributes.dispatcher(
        "akka.stream.materializer.blocking-io-dispatcher"))
  }

  def toInputStream(objectLocation: ObjectLocation)(
    implicit s3Client: AmazonS3): Try[InputStream] = Try {
    s3Client
      .getObject(objectLocation.namespace, objectLocation.key)
      .getObjectContent
  }

}
