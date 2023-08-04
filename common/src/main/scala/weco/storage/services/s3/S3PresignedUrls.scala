package weco.storage.services.s3

import software.amazon.awssdk.services.s3.model.GetObjectRequest
import software.amazon.awssdk.services.s3.presigner.S3Presigner
import software.amazon.awssdk.services.s3.presigner.model.GetObjectPresignRequest
import weco.storage.s3.S3ObjectLocation
import weco.storage.{ReadError, StoreReadError}

import java.net.URL
import java.{time => JavaTime}
import scala.concurrent.duration.Duration
import scala.util.{Failure, Success, Try}

class S3PresignedUrls(implicit s3Presigner: S3Presigner) {
  def getPresignedGetURL(
    location: S3ObjectLocation,
    expiryLength: Duration
  ): Either[ReadError, URL] = {

    val getRequest =
      GetObjectRequest
        .builder()
        .bucket(location.bucket)
        .key(location.key)
        .build()

    val presignRequest =
      GetObjectPresignRequest
        .builder()
        .getObjectRequest(getRequest)
        .signatureDuration(JavaTime.Duration.ofSeconds(expiryLength.toSeconds))
        .build()

    Try {
      s3Presigner.presignGetObject(presignRequest)
    } match {
      case Success(resp) => Right(resp.url())
      case Failure(err)  => Left(StoreReadError(err))
    }
  }
}
