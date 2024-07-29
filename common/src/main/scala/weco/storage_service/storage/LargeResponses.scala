package weco.storage_service.storage

import java.net.URL

import org.apache.pekko.http.scaladsl.model.headers.{ETag, Location}
import org.apache.pekko.http.scaladsl.model.{HttpResponse, StatusCodes}
import org.apache.pekko.http.scaladsl.server.Directives.mapResponse
import org.apache.pekko.http.scaladsl.server.Route
import org.apache.pekko.stream.Materializer
import grizzled.slf4j.Logging
import weco.storage.providers.s3.S3ObjectLocationPrefix
import weco.storage.services.s3.S3Uploader
import weco.storage.streaming.InputStreamWithLength

import scala.concurrent.duration.Duration

trait LargeResponses extends Logging {

  import java.util.UUID

  import org.apache.pekko.stream.scaladsl.StreamConverters

  val s3Uploader: S3Uploader
  val s3Prefix: S3ObjectLocationPrefix

  val maximumResponseByteLength: Long
  val cacheDuration: Duration

  private val converter = StreamConverters.asInputStream()

  implicit val materializer: Materializer

  def wrapLargeResponses(route: Route): Route =
    mapResponse(storeAndRedirect)(route)

  private def storeAndRedirect(response: HttpResponse): HttpResponse = {
    response.entity.contentLengthOption match {
      case Some(length) if length > maximumResponseByteLength => {
        val storageKey = response
          .header[ETag]
          .map(_.value.replace("\"", ""))
          .getOrElse(UUID.randomUUID().toString)

        debug(
          msg =
            s"Attempting to return large object ($length > $maximumResponseByteLength): assigning key $storageKey"
        )

        val objectLocation = s3Prefix.asLocation(storageKey)

        val inputStream = response.entity
          .getDataBytes()
          .runWith(converter, materializer)

        val content = new InputStreamWithLength(
          inputStream = inputStream,
          length = length
        )

        val uploaded = s3Uploader.uploadAndGetURL(
          location = objectLocation,
          content = content,
          expiryLength = cacheDuration,
          checkExists = true
        )

        uploaded match {
          case Right(url: URL) =>
            HttpResponse(
              status = StatusCodes.TemporaryRedirect,
              headers = Location(url.toExternalForm) :: Nil
            )
          case Left(err) =>
            error(err)
            HttpResponse(
              status = StatusCodes.InternalServerError
            )
        }
      }

      case _ => response
    }
  }
}
