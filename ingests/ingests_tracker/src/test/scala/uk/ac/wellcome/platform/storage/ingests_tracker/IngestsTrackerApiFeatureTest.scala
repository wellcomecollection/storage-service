package uk.ac.wellcome.platform.storage.ingests_tracker

import java.time.Instant

import akka.http.scaladsl.Http
import akka.http.scaladsl.model.HttpMethods._
import akka.http.scaladsl.model._
import io.circe.syntax._
import org.scalatest.EitherValues
import org.scalatest.concurrent.IntegrationPatience
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import uk.ac.wellcome.akka.fixtures.Akka
import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.json.utils.JsonAssertions
import uk.ac.wellcome.platform.archive.common.fixtures.{
  HttpFixtures,
  StorageRandomThings
}
import uk.ac.wellcome.platform.archive.common.ingests.models.Ingest.{
  Failed,
  Succeeded
}
import uk.ac.wellcome.platform.archive.common.ingests.models._
import uk.ac.wellcome.platform.storage.ingests_tracker.fixtures.IngestsTrackerApiFixture
import uk.ac.wellcome.platform.storage.ingests_tracker.tracker.IngestDoesNotExistError

class IngestsTrackerApiFeatureTest
    extends AnyFunSpec
    with Matchers
    with Akka
    with IngestsTrackerApiFixture
    with JsonAssertions
    with HttpFixtures
    with IntegrationPatience
    with EitherValues
    with StorageRandomThings {

  describe("GET /healthcheck") {
    val path = s"http://localhost:8080/healthcheck"

    it("responds OK") {
      withIngestsTrackerApi() { _ =>
        whenGetRequestReady(path) { result =>
          result.status shouldBe StatusCodes.OK
        }
      }
    }
  }

  val badEntity = HttpEntity(
    ContentTypes.`application/json`,
    randomAlphanumeric
  )

  describe("POST /ingest") {
    val path = "http://localhost:8080/ingest"

    val ingest = createIngestWith(
      createdDate = Instant.now(),
      version = None
    )

    val ingestEntity = HttpEntity(
      ContentTypes.`application/json`,
      ingest.asJson.noSpaces
    )

    describe("with a valid Ingest") {
      withIngestsTrackerApi() {
        case (callbackSender, ingestsSender, ingestTracker) =>
          whenPostRequestReady(path, ingestEntity) { response =>
            it("responds Created") {
              response.status shouldBe StatusCodes.Created
            }

            it("stores the Ingest") {
              val getIngest = ingestTracker.get(ingest.id)

              getIngest shouldBe a[Right[_, _]]
              getIngest.right.get.identifiedT shouldBe ingest
            }

            it("sends an Ingest message") {
              ingestsSender.getMessages[Ingest] shouldBe Seq(ingest)
            }

            it("does not send a CallbackNotification message") {
              callbackSender.getMessages[CallbackNotification] shouldBe empty
            }
          }
      }
    }

    describe("with an existing Ingest") {
      withIngestsTrackerApi(Seq(ingest)) {
        case (callbackSender, ingestsSender, ingestTracker) =>
          whenPostRequestReady(path, ingestEntity) { response =>
            it("responds Conflict") {
              response.status shouldBe StatusCodes.Conflict
            }

            it("does not modify the Ingest") {
              val getIngest = ingestTracker.get(ingest.id)

              getIngest shouldBe a[Right[_, _]]
              getIngest.right.get.identifiedT shouldBe ingest
            }

            it("does not send an Ingest message") {
              ingestsSender.getMessages[Ingest] shouldBe empty
            }

            it("does not send a CallbackNotification message") {
              callbackSender.getMessages[CallbackNotification] shouldBe empty
            }
          }
      }
    }

    describe("with bad JSON") {
      withIngestsTrackerApi(Seq(ingest)) {
        case (callbackSender, ingestsSender, ingestTracker) =>
          whenPostRequestReady(path, badEntity) { response =>
            it("responds BadRequest") {
              response.status shouldBe StatusCodes.BadRequest
            }

            it("does not modify the Ingest") {
              val getIngest = ingestTracker.get(ingest.id)

              getIngest shouldBe a[Right[_, _]]
              getIngest.right.get.identifiedT shouldBe ingest
            }

            it("does not send an Ingest message") {
              ingestsSender.getMessages[Ingest] shouldBe empty
            }

            it("does not send a CallbackNotification message") {
              callbackSender.getMessages[CallbackNotification] shouldBe empty
            }
          }
      }
    }

    describe("when broken") {
      withBrokenIngestsTrackerApi {
        case (callbackSender, ingestsSender, _) =>
          whenPostRequestReady(path, ingestEntity) { response =>
            it("responds InternalServerError") {
              response.status shouldBe StatusCodes.InternalServerError
            }
          }

          it("does not send an Ingest message") {
            ingestsSender.getMessages[Ingest] shouldBe empty
          }

          it("does not send a CallbackNotification message") {
            callbackSender.getMessages[CallbackNotification] shouldBe empty
          }
      }
    }
  }

  describe("PATCH /ingest/:id") {
    val ingest = createIngestWith(
      createdDate = Instant.now(),
      events = Seq(createIngestEvent, createIngestEvent),
      version = None
    )

    val path = s"http://localhost:8080/ingest/${ingest.id}"

    val ingestEvent = createIngestEventUpdateWith(id = ingest.id)

    val ingestEventEntity = HttpEntity(
      ContentTypes.`application/json`,
      toJson[IngestUpdate](ingestEvent).get
    )

    val ingestStatusUpdateSucceeded =
      createIngestStatusUpdateWith(
        id = ingest.id,
        status = Succeeded
      )

    val ingestStatusUpdateSucceededEntity = HttpEntity(
      ContentTypes.`application/json`,
      toJson[IngestUpdate](ingestStatusUpdateSucceeded).get
    )

    val ingestStatusUpdateFailed =
      createIngestStatusUpdateWith(
        id = ingest.id,
        status = Failed
      )

    val ingestStatusUpdateFailedEntity = HttpEntity(
      ContentTypes.`application/json`,
      toJson[IngestUpdate](ingestStatusUpdateFailed).get
    )

    describe("with a valid IngestEventUpdate") {
      withIngestsTrackerApi(Seq(ingest)) {
        case (callbackSender, ingestsSender, ingestTracker) =>
          whenPatchRequestReady(path, ingestEventEntity) { response =>
            it("responds OK with an updated Ingest") {
              response.status shouldBe StatusCodes.OK

              withStringEntity(response.entity) { jsonString =>
                val updatedIngestResponse = fromJson[Ingest](jsonString).get
                updatedIngestResponse.events should contain allElementsOf ingestEvent.events
              }
            }

            val ingestFromTracker =
              ingestTracker.get(ingest.id).right.get.identifiedT

            it("adds the IngestEventUpdate to the Ingest") {
              ingestFromTracker.events should contain allElementsOf ingestEvent.events
            }

            it("sends an Ingest message") {
              ingestsSender.getMessages[Ingest] shouldBe Seq(ingestFromTracker)
            }

            it("does not send a CallbackNotification message") {
              callbackSender.getMessages[CallbackNotification] shouldBe empty
            }
          }
      }
    }

    describe("with a valid IngestStatusUpdate to Success") {
      withIngestsTrackerApi(Seq(ingest)) {
        case (callbackSender, ingestsSender, ingestTracker) =>
          whenPatchRequestReady(path, ingestStatusUpdateSucceededEntity) {
            response =>
              it("responds OK when updating with an updated Ingest") {
                response.status shouldBe StatusCodes.OK

                withStringEntity(response.entity) { jsonString =>
                  val updatedIngestResponse = fromJson[Ingest](jsonString).get
                  updatedIngestResponse.status shouldBe ingestStatusUpdateSucceeded.status
                  updatedIngestResponse.events should contain allElementsOf ingestStatusUpdateSucceeded.events
                }
              }

              val ingestFromTracker =
                ingestTracker.get(ingest.id).right.get.identifiedT

              it("adds the IngestStatusUpdate to the Ingest") {
                ingestFromTracker.status shouldBe ingestStatusUpdateSucceeded.status
                ingestFromTracker.events should contain allElementsOf ingestStatusUpdateSucceeded.events
              }

              it("sends an Ingest message") {
                ingestsSender.getMessages[Ingest] shouldBe Seq(
                  ingestFromTracker
                )
              }

              it("sends a CallbackNotification message") {
                val expectedCallbackNotification = CallbackNotification(
                  ingestId = ingestFromTracker.id,
                  callbackUri = ingestFromTracker.callback.get.uri,
                  payload = ingestFromTracker
                )

                callbackSender.getMessages[CallbackNotification] shouldBe Seq(
                  expectedCallbackNotification
                )
              }
          }
      }
    }

    describe("with a valid IngestStatusUpdate to Failure") {
      withIngestsTrackerApi(Seq(ingest)) {
        case (callbackSender, ingestsSender, ingestTracker) =>
          whenPatchRequestReady(path, ingestStatusUpdateFailedEntity) {
            response =>
              it("responds OK when updating with an updated Ingest") {
                response.status shouldBe StatusCodes.OK

                withStringEntity(response.entity) { jsonString =>
                  val updatedIngestResponse = fromJson[Ingest](jsonString).get
                  updatedIngestResponse.status shouldBe ingestStatusUpdateFailed.status
                  updatedIngestResponse.events should contain allElementsOf ingestStatusUpdateFailed.events
                }
              }

              val ingestFromTracker =
                ingestTracker.get(ingest.id).right.get.identifiedT

              it("adds the IngestStatusUpdate to the Ingest") {
                ingestFromTracker.status shouldBe ingestStatusUpdateFailed.status
                ingestFromTracker.events should contain allElementsOf ingestStatusUpdateFailed.events
              }

              it("sends an Ingest message") {
                ingestsSender.getMessages[Ingest] shouldBe Seq(
                  ingestFromTracker
                )
              }

              it("sends a CallbackNotification message") {
                val expectedCallbackNotification = CallbackNotification(
                  ingestId = ingestFromTracker.id,
                  callbackUri = ingestFromTracker.callback.get.uri,
                  payload = ingestFromTracker
                )

                callbackSender.getMessages[CallbackNotification] shouldBe Seq(
                  expectedCallbackNotification
                )
              }
          }
      }
    }

    describe("with an invalid Status transition in IngestStatusUpdate") {
      withIngestsTrackerApi(Seq(ingest)) {
        case (callbackSender, ingestsSender, ingestTracker) =>
          whenPatchRequestReady(path, ingestStatusUpdateSucceededEntity) {
            response =>
              response.status shouldBe StatusCodes.OK

              val succeededIngestFromTracker: Ingest =
                ingestTracker.get(ingest.id).right.get.identifiedT

              succeededIngestFromTracker.status shouldBe Succeeded

              whenPatchRequestReady(path, ingestStatusUpdateFailedEntity) {
                response =>
                  it("responds with Conflict") {
                    response.status shouldBe StatusCodes.Conflict
                  }

                  val ingestFromTracker =
                    ingestTracker.get(ingest.id).right.get.identifiedT

                  it("does not modify the Ingest") {
                    ingestFromTracker shouldBe succeededIngestFromTracker
                  }

                  it("does not send an Ingest message") {
                    ingestsSender.getMessages[Ingest] shouldBe Seq(
                      succeededIngestFromTracker
                    )
                  }

                  it("only sends the expected CallbackNotification message") {
                    val expectedCallbackNotification = CallbackNotification(
                      ingestId = ingestFromTracker.id,
                      callbackUri = ingestFromTracker.callback.get.uri,
                      payload = ingestFromTracker
                    )

                    callbackSender
                      .getMessages[CallbackNotification] shouldBe Seq(
                      expectedCallbackNotification
                    )
                  }
              }
          }
      }
    }

    describe("with an IngestUpdate to non existent Ingest") {
      withIngestsTrackerApi() {
        case (callbackSender, ingestsSender, ingestTracker) =>
          whenPatchRequestReady(path, ingestEventEntity) { response =>
            it("responds with NotFound") {
              response.status shouldBe StatusCodes.NotFound
            }

            it("does not create the Ingest") {
              val getIngest = ingestTracker.get(ingestEvent.id)

              getIngest shouldBe a[Left[_, _]]
              getIngest.left.get shouldBe a[IngestDoesNotExistError]
            }

            it("does not send an Ingest message") {
              ingestsSender.getMessages[Ingest] shouldBe empty
            }

            it("does not send a CallbackNotification message") {
              callbackSender.getMessages[CallbackNotification] shouldBe empty
            }
          }
      }
    }

    describe("when broken") {
      withBrokenIngestsTrackerApi {
        case (callbackSender, ingestsSender, _) =>
          whenPatchRequestReady(path, ingestEventEntity) { response =>
            it("responds InternalServerError") {
              response.status shouldBe StatusCodes.InternalServerError
            }

            it("does not send an Ingest message") {
              ingestsSender.getMessages[Ingest] shouldBe empty
            }

            it("does not send a CallbackNotification message") {
              callbackSender.getMessages[CallbackNotification] shouldBe empty
            }
          }
      }
    }

    describe("with bad JSON") {
      withIngestsTrackerApi(Seq(ingest)) {
        case (callbackSender, ingestsSender, _) =>
          whenPatchRequestReady(path, badEntity) { response =>
            it("responds BadRequest") {
              response.status shouldBe StatusCodes.BadRequest
            }

            it("does not send an Ingest message") {
              ingestsSender.getMessages[Ingest] shouldBe empty
            }

            it("does not send a CallbackNotification message") {
              callbackSender.getMessages[CallbackNotification] shouldBe empty
            }
          }
      }
    }
  }

  describe("GET /ingest/:id") {
    val ingest = createIngestWith(
      createdDate = Instant.now(),
      events = Seq(createIngestEvent, createIngestEvent),
      version = None
    )

    val notRecordedIngestId = createIngestID

    val badPath = s"http://localhost:8080/ingest/$notRecordedIngestId"
    val path = s"http://localhost:8080/ingest/${ingest.id}"

    it("responds NotFound when there is no ingest") {
      withIngestsTrackerApi(Seq(ingest)) { _ =>
        whenGetRequestReady(badPath) { response =>
          response.status shouldBe StatusCodes.NotFound
        }
      }
    }

    it("responds OK with an Ingest when available") {
      withIngestsTrackerApi(Seq(ingest)) { _ =>
        whenGetRequestReady(path) { response =>
          response.status shouldBe StatusCodes.OK

          // We are interested in whether we can serialise/de-serialise
          // what the JSON looks like is not interesting, so we
          // do not need to test in detail as with the external API
          withStringEntity(response.entity) { jsonString =>
            fromJson[Ingest](jsonString).get shouldBe ingest
          }
        }
      }
    }

    it("responds InternalServerError when broken") {
      withBrokenIngestsTrackerApi { _ =>
        whenGetRequestReady(path) { response =>
          response.status shouldBe StatusCodes.InternalServerError
        }
      }
    }
  }

  // Required as not yet provided by HTTP Fixtures
  def whenPatchRequestReady[R](url: String, entity: RequestEntity)(
    testWith: TestWith[HttpResponse, R]
  ): R = {
    withActorSystem { implicit actorSystem =>
      val r = HttpRequest(
        method = PATCH,
        uri = url,
        headers = Nil,
        entity = entity
      )

      val request = Http().singleRequest(r)

      whenReady(request) { response: HttpResponse =>
        testWith(response)
      }
    }
  }
}
