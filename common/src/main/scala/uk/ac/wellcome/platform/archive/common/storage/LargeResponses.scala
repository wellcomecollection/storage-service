package uk.ac.wellcome.platform.archive.common.storage

import java.net.URL

import akka.http.scaladsl.model.headers.{ETag, Location}
import akka.http.scaladsl.model.{HttpResponse, StatusCodes}
import akka.http.scaladsl.server.Directives.mapResponse
import akka.http.scaladsl.server.Route
import akka.stream.ActorMaterializer
import grizzled.slf4j.Logging
import uk.ac.wellcome.platform.archive.common.storage.services.S3Uploader
import uk.ac.wellcome.storage.ObjectLocationPrefix
import uk.ac.wellcome.storage.streaming.InputStreamWithLengthAndMetadata

import scala.concurrent.duration.Duration

trait LargeResponses extends Logging {

  import java.util.UUID

  import akka.stream.scaladsl.StreamConverters

  val s3Uploader: S3Uploader
  val maximumResponseByteLength: Long
  val prefix: ObjectLocationPrefix
  val cacheDuration: Duration

  private val converter = StreamConverters.asInputStream()

  implicit val materializer: ActorMaterializer

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

        val objectLocation = prefix.asLocation(storageKey)

        val inputStream = response.entity
          .getDataBytes()
          .runWith(converter, materializer)

        val content = new InputStreamWithLengthAndMetadata(
          inputStream = inputStream,
          length = length,
          metadata = Map.empty
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
