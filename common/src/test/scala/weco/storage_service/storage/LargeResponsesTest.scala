package weco.storage_service.storage

import java.util.UUID

import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.http.scaladsl.Http
import org.apache.pekko.http.scaladsl.model.headers.{ETag, Location}
import org.apache.pekko.http.scaladsl.model.{
  HttpHeader,
  HttpRequest,
  HttpResponse,
  StatusCodes
}
import org.apache.pekko.http.scaladsl.server.Directives._
import org.apache.pekko.stream.Materializer
import org.apache.pekko.stream.scaladsl.StreamConverters
import org.apache.commons.io.IOUtils
import org.scalatest.funspec.AnyFunSpec
import weco.pekko.fixtures.Pekko
import weco.fixtures.TestWith
import weco.storage.fixtures.S3Fixtures
import weco.storage.fixtures.S3Fixtures.Bucket
import weco.storage.providers.s3.S3ObjectLocationPrefix
import weco.storage.services.s3.S3Uploader

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

class LargeResponsesTest extends AnyFunSpec with S3Fixtures with Pekko {

  private val converter = StreamConverters.asInputStream()

  describe("LargeResponsesTest") {
    it("does not redirect a < max-length response") {
      withLocalS3Bucket { bucket =>
        withActorSystem { implicit actorSystem =>
          withMaterializer { implicit mat =>
            val maxBytes = 100
            val expectedByteArray = randomBytes(maxBytes - 10)

            withLargeResponderResult(bucket, maxBytes, expectedByteArray) {
              response: HttpResponse =>
                val inputStream = response.entity
                  .getDataBytes()
                  .runWith(converter, mat)

                val actualByteArray = IOUtils.toByteArray(inputStream)

                response.status shouldBe StatusCodes.OK
                actualByteArray shouldBe expectedByteArray
            }
          }
        }
      }
    }

    describe("redirects a > max-length response") {
      it("without Etag") {
        withLocalS3Bucket { bucket =>
          withActorSystem { implicit actorSystem =>
            withMaterializer { implicit mat =>
              val maxBytes = 100
              val expectedByteArray = randomBytes(maxBytes + 10)
              val prefix = randomAlphanumeric()

              withLargeResponderResult(
                bucket,
                maxBytes,
                expectedByteArray,
                prefix
              ) { response: HttpResponse =>
                response.status shouldBe StatusCodes.TemporaryRedirect

                val redirectLocation = response.header[Location].get

                val madeRequest = Http()
                  .singleRequest(HttpRequest(uri = redirectLocation.uri))

                val madeRequestResult: HttpResponse =
                  Await.result(madeRequest, 10.seconds)

                madeRequestResult.status shouldBe StatusCodes.OK
                val inputStream = madeRequestResult.entity
                  .getDataBytes()
                  .runWith(converter, mat)

                val actualByteArray = IOUtils.toByteArray(inputStream)

                actualByteArray shouldBe expectedByteArray

                val keys = listKeysInBucket(bucket)
                keys should have length 1

                val keyParts = keys.head.split("/")
                keyParts should have length 2

                keyParts.head shouldBe prefix
                UUID.fromString(keyParts(1)) shouldBe a[UUID]
              }
            }
          }
        }
      }

      it("with Etag") {
        withLocalS3Bucket { bucket =>
          withActorSystem { implicit actorSystem =>
            withMaterializer { implicit mat =>
              val maxBytes = 100
              val expectedByteArray = randomBytes(maxBytes + 10)
              val header = ETag(randomAlphanumeric())
              val prefix = randomAlphanumeric()

              withLargeResponderResult(
                bucket,
                maxBytes,
                expectedByteArray,
                prefix,
                List(header)
              ) { response: HttpResponse =>
                response.status shouldBe StatusCodes.TemporaryRedirect

                val redirectLocation = response.header[Location].get

                val madeRequest = Http()
                  .singleRequest(HttpRequest(uri = redirectLocation.uri))

                val madeRequestResult: HttpResponse =
                  Await.result(madeRequest, 10.seconds)

                madeRequestResult.status shouldBe StatusCodes.OK
                val inputStream = madeRequestResult.entity
                  .getDataBytes()
                  .runWith(converter, mat)

                val actualByteArray = IOUtils.toByteArray(inputStream)

                actualByteArray shouldBe expectedByteArray

                val keys = listKeysInBucket(bucket)

                keys should have length 1

                val keyParts = keys.head.split("/")
                keyParts should have length 2

                keyParts.head shouldBe prefix
                keyParts.tail.head shouldBe header.value().replace("\"", "")
              }
            }
          }
        }
      }
    }
  }

  private def withLargeResponderResult[R](
    bucket: Bucket,
    maxBytes: Int,
    testBytes: Array[Byte],
    prefix: String = randomAlphanumeric(),
    headers: List[HttpHeader] = Nil
  )(testWith: TestWith[HttpResponse, R])(
    implicit mat: Materializer,
    as: ActorSystem
  ): R = {

    val port = 8080
    val interface = "127.0.0.1"

    val uploader = new S3Uploader()

    val duration = 30 seconds

    val largeResponder = new LargeResponses {
      override val s3Uploader: S3Uploader = uploader
      override val s3Prefix: S3ObjectLocationPrefix =
        S3ObjectLocationPrefix(
          bucket = bucket.name,
          keyPrefix = prefix
        )

      override val maximumResponseByteLength: Long = maxBytes
      override val cacheDuration: Duration = duration
      override implicit val materializer: Materializer = mat
    }

    val routes = largeResponder.wrapLargeResponses(get {
      respondWithHeaders(headers) {
        complete(testBytes)
      }
    })

    val binding: Future[Http.ServerBinding] =
      Http().newServerAt(interface, port).bindFlow(routes)

    val madeRequest = Http().singleRequest(
      HttpRequest(uri = s"http://$interface:$port")
    )

    // This can sometimes flake out when running in Travis CI.  The deadline
    // is somewhat arbitrary, so if it keeps failing, bump this deadline again.
    val madeRequestResult: HttpResponse =
      Await.result(madeRequest, atMost = 5.seconds)

    val result = testWith(madeRequestResult)

    Await
      .result(binding, atMost = 5.seconds)
      .terminate(hardDeadline = 5.seconds)
      .flatMap { _ =>
        as.terminate()
      }

    result
  }
}
