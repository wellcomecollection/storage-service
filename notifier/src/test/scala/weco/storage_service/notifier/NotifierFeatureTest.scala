package weco.storage_service.notifier

import org.apache.pekko.http.scaladsl.model.{HttpRequest, HttpResponse, StatusCodes}
import org.scalatest.Inside
import org.scalatest.concurrent.{Eventually, IntegrationPatience, ScalaFutures}
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import weco.pekko.fixtures.Pekko
import weco.fixtures.TimeAssertions
import weco.http.client.HttpClient
import weco.http.fixtures.HttpFixtures
import weco.json.JsonUtil._
import weco.storage_service.bagit.models.BagVersion
import weco.storage_service.generators.IngestGenerators
import weco.storage_service.ingests.models._
import weco.storage_service.notifier.fixtures.NotifierFixtures

import java.net.URI
import scala.concurrent.Future
import scala.language.reflectiveCalls

class NotifierFeatureTest
    extends AnyFunSpec
    with Matchers
    with ScalaFutures
    with Pekko
    with IntegrationPatience
    with NotifierFixtures
    with Inside
    with IngestGenerators
    with TimeAssertions
    with HttpFixtures
    with Eventually {

  describe("Making callbacks") {
    it("makes a POST request when it receives an Ingest with a callback") {
      val recordingClient = new HttpClient {
        var requests: Seq[HttpRequest] = Seq()

        override def singleRequest(r: HttpRequest): Future[HttpResponse] =
          synchronized {
            requests = requests :+ r
            Future.successful(HttpResponse())
          }
      }

      withNotifier(recordingClient) {
        case (queue, _) =>
          val ingestId = createIngestID

          val callbackUri = new URI(s"http://example.org/callback/$ingestId")

          val ingest = createIngestWith(
            id = ingestId,
            callback = Some(createCallbackWith(uri = callbackUri)),
            events = createIngestEvents(count = 2),
            version = None
          )

          sendNotificationToSQS(
            queue,
            CallbackNotification(ingestId, callbackUri, ingest)
          )

          val ingestLocation =
            ingest.sourceLocation.asInstanceOf[S3SourceLocation]

          val expectedJson =
            s"""
             |{
             |  "id": "${ingest.id.toString}",
             |  "type": "Ingest",
             |  "ingestType": {
             |    "id": "${ingest.ingestType.id}",
             |    "type": "IngestType"
             |  },
             |  "space": {
             |    "id": "${ingest.space.underlying}",
             |    "type": "Space"
             |  },
             |  "bag": {
             |    "type": "Bag",
             |    "info": {
             |      "type": "BagInfo",
             |      "externalIdentifier": "${ingest.externalIdentifier.underlying}"
             |    }
             |  },
             |  "status": {
             |    "id": "${ingest.status.toString}",
             |    "type": "Status"
             |  },
             |  "sourceLocation": {
             |    "type": "Location",
             |    "provider": {
             |      "type": "Provider",
             |      "id": "amazon-s3"
             |    },
             |    "bucket": "${ingestLocation.location.bucket}",
             |    "path": "${ingestLocation.location.key}"
             |  },
             |  "callback": {
             |    "type": "Callback",
             |    "url": "${ingest.callback.get.uri}",
             |    "status": {
             |      "id": "${ingest.callback.get.status.toString}",
             |      "type": "Status"
             |    }
             |  },
             |  "createdDate": "${ingest.createdDate}",
             |  "lastModifiedDate": "${ingest.lastModifiedDate.get}",
             |  "events": [
             |    {
             |      "type": "IngestEvent",
             |      "createdDate": "${ingest.events(0).createdDate}",
             |      "description": "${ingest.events(0).description}"
             |    },
             |    {
             |      "type": "IngestEvent",
             |      "createdDate": "${ingest.events(1).createdDate}",
             |      "description": "${ingest.events(1).description}"
             |    }
             |  ]
             |}
                 """.stripMargin

          eventually {
            recordingClient.requests should have size 1

            val request = recordingClient.requests.head
            request.uri.toString shouldBe callbackUri.toString

            withStringEntity(request.entity) {
              assertJsonStringsAreEqual(_, expectedJson)
            }
          }
      }
    }
  }

  import org.scalatest.prop.TableDrivenPropertyChecks._

  val successfulStatuscodes =
    Table(
      "status code",
      StatusCodes.OK,
      StatusCodes.Created,
      StatusCodes.Accepted,
      StatusCodes.NoContent
    )
  describe("Updating status") {
    it("sends an IngestUpdate when it receives a successful callback") {
      forAll(successfulStatuscodes) { statusCode =>
        val client = new HttpClient {
          override def singleRequest(
            request: HttpRequest
          ): Future[HttpResponse] =
            Future.successful(HttpResponse(status = statusCode))
        }

        withNotifier(client) {
          case (queue, messageSender) =>
            val ingestID = createIngestID

            val callbackUri = new URI(s"http://example.org/callback/$ingestID")

            val ingest = createIngestWith(
              id = ingestID,
              callback = Some(createCallbackWith(uri = callbackUri)),
              events = createIngestEvents(count = 2),
              version = Some(BagVersion(2))
            )

            sendNotificationToSQS(
              queue,
              CallbackNotification(ingestID, callbackUri, ingest)
            )

            eventually {
              val updates = messageSender.getMessages[IngestUpdate]
              updates should have size 1
              val receivedUpdate = updates.head

              inside(receivedUpdate) {
                case IngestCallbackStatusUpdate(
                    id,
                    callbackStatus,
                    List(ingestEvent)
                    ) =>
                  id shouldBe ingest.id
                  ingestEvent.description shouldBe "Callback fulfilled"
                  callbackStatus shouldBe Callback.Succeeded
                  assertRecent(ingestEvent.createdDate)
              }
            }
        }
      }
    }

    it("sends an IngestUpdate when the callback returns a 404") {
      val client = new HttpClient {
        override def singleRequest(request: HttpRequest): Future[HttpResponse] =
          Future.successful(HttpResponse(status = StatusCodes.NotFound))
      }

      withNotifier(client) {
        case (queue, messageSender) =>
          val ingestId = createIngestID

          val callbackUri = new URI(s"http://example.org/callback/$ingestId")

          val ingest = createIngestWith(
            id = ingestId,
            callback = Some(createCallbackWith(uri = callbackUri))
          )

          sendNotificationToSQS[CallbackNotification](
            queue,
            CallbackNotification(ingestId, callbackUri, ingest)
          )

          eventually {
            val updates = messageSender.getMessages[IngestUpdate]
            updates should have size 1
            val receivedUpdate = updates.head

            inside(receivedUpdate) {
              case IngestCallbackStatusUpdate(
                  id,
                  callbackStatus,
                  List(ingestEvent)
                  ) =>
                id shouldBe ingest.id
                ingestEvent.description shouldBe s"Callback failed for: ${ingest.id}, got 404 Not Found!"
                callbackStatus shouldBe Callback.Failed
                assertRecent(ingestEvent.createdDate)
            }
          }
      }
    }

    it("sends an IngestUpdate if the callback fails") {
      val client = new HttpClient {
        override def singleRequest(request: HttpRequest): Future[HttpResponse] =
          Future.failed(new Throwable("BOOM!"))
      }

      withNotifier(client) {
        case (queue, messageSender) =>
          val ingestId = createIngestID

          val callbackUri = new URI(s"http://example.org/callback/$ingestId")

          val ingest = createIngestWith(
            id = ingestId,
            callback = Some(createCallbackWith(uri = callbackUri))
          )

          sendNotificationToSQS[CallbackNotification](
            queue,
            CallbackNotification(ingestId, callbackUri, ingest)
          )

          eventually {
            val updates = messageSender.getMessages[IngestUpdate]
            updates should have size 1
            val receivedUpdate = updates.head

            inside(receivedUpdate) {
              case IngestCallbackStatusUpdate(
                  id,
                  callbackStatus,
                  List(ingestEvent)
                  ) =>
                id shouldBe ingest.id
                ingestEvent.description shouldBe s"Callback failed for: ${ingest.id} (BOOM!)"
                callbackStatus shouldBe Callback.Failed
                assertRecent(ingestEvent.createdDate)
            }
          }
      }
    }
  }
}
