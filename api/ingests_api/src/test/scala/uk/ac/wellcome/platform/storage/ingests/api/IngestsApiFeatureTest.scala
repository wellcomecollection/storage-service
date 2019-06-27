package uk.ac.wellcome.platform.storage.ingests.api

import java.time.Instant
import java.util.UUID

import akka.http.scaladsl.model._
import akka.http.scaladsl.unmarshalling.Unmarshal
import de.heikoseeberger.akkahttpcirce.FailFastCirceSupport._
import io.circe.optics.JsonPath.root
import io.circe.parser._
import org.scalatest.concurrent.{IntegrationPatience, ScalaFutures}
import org.scalatest.{FunSpec, Inside, Matchers}
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.json.utils.JsonAssertions
import uk.ac.wellcome.platform.archive.common.http.HttpMetricResults
import uk.ac.wellcome.platform.archive.common.ingests.fixtures.IngestTrackerFixture
import uk.ac.wellcome.platform.archive.common.ingests.models._
import uk.ac.wellcome.platform.archive.common.SourceLocationPayload
import uk.ac.wellcome.platform.archive.common.bagit.models.ExternalIdentifier
import uk.ac.wellcome.platform.archive.common.fixtures.StorageRandomThings
import uk.ac.wellcome.platform.archive.common.storage.models.StorageSpace
import uk.ac.wellcome.platform.archive.display._
import uk.ac.wellcome.platform.storage.ingests.api.fixtures.IngestsApiFixture
import uk.ac.wellcome.storage.ObjectLocation
import uk.ac.wellcome.storage.dynamo._

import scala.util.Success

class IngestsApiFeatureTest
    extends FunSpec
    with Matchers
    with ScalaFutures
    with IngestTrackerFixture
    with IngestsApiFixture
    with Inside
    with IntegrationPatience
    with JsonAssertions
    with StorageRandomThings {

  val contextUrl = "http://api.wellcomecollection.org/storage/v1/context.json"
  describe("GET /ingests/:id") {
    it("returns a ingest tracker when available") {
      withConfiguredApp {
        case (table, _, metricsSender, baseUrl) =>
          withMaterializer { implicit materializer =>
            withIngestTracker(table) { ingestTracker =>
              val ingest = createIngest

              val expectedSourceLocationJson =
                s"""{
                  "provider": {
                    "id": "${StandardDisplayProvider.id}",
                    "type": "Provider"
                  },
                  "bucket": "${ingest.sourceLocation.location.namespace}",
                  "path": "${ingest.sourceLocation.location.key}",
                  "type": "Location"
                }""".stripMargin

              val expectedCallbackJson =
                s"""{
                  "url": "${ingest.callback.get.uri}",
                  "status": {
                    "id": "processing",
                    "type": "Status"
                  },
                  "type": "Callback"
                }""".stripMargin

              val expectedIngestTypeJson = s"""{
                "id": "create",
                "type": "IngestType"
              }""".stripMargin

              val expectedSpaceJson = s"""{
                "id": "${ingest.space.underlying}",
                "type": "Space"
              }""".stripMargin

              val expectedStatusJson = s"""{
                "id": "accepted",
                "type": "Status"
              }""".stripMargin

              ingestTracker.initialise(ingest) shouldBe a[Success[_]]

              whenGetRequestReady(s"$baseUrl/ingests/${ingest.id}") { result =>
                result.status shouldBe StatusCodes.OK

                withStringEntity(result.entity) { jsonString =>
                  val json = parse(jsonString).right.get
                  root.`@context`.string
                    .getOption(json)
                    .get shouldBe "http://api.wellcomecollection.org/storage/v1/context.json"
                  root.id.string
                    .getOption(json)
                    .get shouldBe ingest.id.toString

                  assertJsonStringsAreEqual(
                    root.sourceLocation.json.getOption(json).get.noSpaces,
                    expectedSourceLocationJson)

                  assertJsonStringsAreEqual(
                    root.callback.json.getOption(json).get.noSpaces,
                    expectedCallbackJson)
                  assertJsonStringsAreEqual(
                    root.ingestType.json.getOption(json).get.noSpaces,
                    expectedIngestTypeJson)
                  assertJsonStringsAreEqual(
                    root.space.json.getOption(json).get.noSpaces,
                    expectedSpaceJson)
                  assertJsonStringsAreEqual(
                    root.status.json.getOption(json).get.noSpaces,
                    expectedStatusJson)
                  assertJsonStringsAreEqual(
                    root.events.json.getOption(json).get.noSpaces,
                    "[]")

                  root.`type`.string.getOption(json).get shouldBe "Ingest"

                  assertRecent(
                    Instant.parse(root.createdDate.string.getOption(json).get),
                    25)
                }

                assertMetricSent(
                  metricsSender,
                  result = HttpMetricResults.Success)
              }
            }
          }
      }
    }

    it("does not output empty values") {
      withConfiguredApp {
        case (table, _, metricsSender, baseUrl) =>
          withMaterializer { implicit materialiser =>
            withIngestTracker(table) { ingestTracker =>
              val ingest = createIngestWith(callback = None)
              ingestTracker.initialise(ingest) shouldBe a[Success[_]]
              whenGetRequestReady(s"$baseUrl/ingests/${ingest.id}") { result =>
                result.status shouldBe StatusCodes.OK
                withStringEntity(result.entity) { jsonString =>
                  val infoJson = parse(jsonString).right.get
                  infoJson.findAllByKey("callback") shouldBe empty
                }

                assertMetricSent(
                  metricsSender,
                  result = HttpMetricResults.Success)
              }
            }
          }
      }
    }

    it("returns a 404 NotFound if no ingest tracker matches id") {
      withMaterializer { implicit materializer =>
        withConfiguredApp {
          case (_, _, metricsSender, baseUrl) =>
            val id = randomUUID
            whenGetRequestReady(s"$baseUrl/ingests/$id") { response =>
              assertIsUserErrorResponse(
                response,
                description = s"Ingest $id not found",
                statusCode = StatusCodes.NotFound,
                label = "Not Found"
              )

              assertMetricSent(
                metricsSender,
                result = HttpMetricResults.UserError)
            }
        }
      }
    }

    it("returns a 500 Server Error if reading from DynamoDB fails") {
      withMaterializer { implicit materializer =>
        withBrokenApp {
          case (_, _, metricsSender, baseUrl) =>
            whenGetRequestReady(s"$baseUrl/ingests/$randomUUID") { response =>
              assertIsInternalServerErrorResponse(response)

              assertMetricSent(
                metricsSender,
                result = HttpMetricResults.ServerError)
            }
        }
      }
    }
  }

  def createRequestWith(
    ingestType: String = "create",
    bucket: String = randomAlphanumeric(),
    key: String = randomAlphanumeric(),
    space: String = randomAlphanumeric(),
    externalIdentifier: ExternalIdentifier = createExternalIdentifier
  ): RequestEntity =
    HttpEntity(
      ContentTypes.`application/json`,
      s"""|{
          |  "type": "Ingest",
          |  "ingestType": {
          |    "id": "$ingestType",
          |    "type": "IngestType"
          |  },
          |  "sourceLocation":{
          |    "type": "Location",
          |    "provider": {
          |      "type": "Provider",
          |      "id": "${StandardDisplayProvider.id}"
          |    },
          |    "bucket": "$bucket",
          |    "path": "$key"
          |  },
          |  "space": {
          |    "id": "$space",
          |    "type": "Space"
          |  },
          |  "callback": {
          |    "url": "${testCallbackUri.toString}"
          |  },
          |  "externalIdentifier": "${externalIdentifier.underlying}"
          |}""".stripMargin
    )

  describe("POST /ingests") {
    it("creates an ingest") {
      withConfiguredApp {
        case (table, messageSender, metricsSender, baseUrl) =>
          withMaterializer { implicit mat =>
            val url = s"$baseUrl/ingests"

            val bucketName = "bucket"
            val s3key = "key.txt"
            val spaceName = "somespace"

            val externalIdentifier = createExternalIdentifier

            val entity = createRequestWith(
              bucket = bucketName,
              key = s3key,
              space = spaceName,
              externalIdentifier = externalIdentifier
            )

            val expectedLocationR = s"$baseUrl/(.+)".r

            whenPostRequestReady(url, entity) { response: HttpResponse =>
              response.status shouldBe StatusCodes.Created

              val maybeId = response.headers.collectFirst {
                case HttpHeader("location", expectedLocationR(id)) => id
              }

              maybeId.isEmpty shouldBe false
              val id = UUID.fromString(maybeId.get)

              val ingestFuture =
                Unmarshal(response.entity).to[ResponseDisplayIngest]

              whenReady(ingestFuture) { actualIngest =>
                actualIngest.context shouldBe contextUrl
                actualIngest.id shouldBe id
                actualIngest.sourceLocation shouldBe DisplayLocation(
                  provider = StandardDisplayProvider,
                  bucket = bucketName,
                  path = s3key
                )

                actualIngest.callback.isDefined shouldBe true
                actualIngest.callback.get.url shouldBe testCallbackUri.toString
                actualIngest.callback.get.status.get shouldBe DisplayStatus(
                  "processing")

                actualIngest.ingestType shouldBe CreateDisplayIngestType

                actualIngest.status shouldBe DisplayStatus("accepted")

                actualIngest.space shouldBe DisplayStorageSpace(
                  spaceName,
                  "Space")

                actualIngest.externalIdentifier shouldBe externalIdentifier.underlying

                val expectedIngest = Ingest(
                  id = IngestID(id),
                  ingestType = CreateIngestType,
                  sourceLocation = StorageLocation(
                    StandardStorageProvider,
                    ObjectLocation(bucketName, s3key)
                  ),
                  space = StorageSpace(spaceName),
                  callback = Some(Callback(testCallbackUri, Callback.Pending)),
                  status = Ingest.Accepted,
                  externalIdentifier = externalIdentifier,
                  createdDate = Instant.parse(actualIngest.createdDate),
                  lastModifiedDate = None,
                  events = Nil
                )

                assertTableOnlyHasItem[Ingest](expectedIngest, table)

                val expectedPayload = SourceLocationPayload(expectedIngest)
                messageSender
                  .getMessages[SourceLocationPayload] shouldBe Seq(
                  expectedPayload)

                assertMetricSent(
                  metricsSender,
                  result = HttpMetricResults.Success)
              }
            }
          }
      }
    }

    it("allows requesting an ingestType 'create'") {
      withConfiguredApp {
        case (_, messageSender, metricsSender, baseUrl) =>
          withMaterializer { implicit materializer =>
            val url = s"$baseUrl/ingests"

            val entity = createRequestWith(
              ingestType = "create"
            )

            whenPostRequestReady(url, entity) { response: HttpResponse =>
              response.status shouldBe StatusCodes.Created

              val ingestFuture =
                Unmarshal(response.entity).to[ResponseDisplayIngest]

              whenReady(ingestFuture) { actualIngest =>
                actualIngest.ingestType.id shouldBe "create"

                val payload =
                  messageSender.getMessages[SourceLocationPayload].head
                payload.context.ingestType shouldBe CreateIngestType

                assertMetricSent(
                  metricsSender,
                  result = HttpMetricResults.Success)
              }
            }
          }
      }
    }

    it("allows requesting an ingestType 'update'") {
      withConfiguredApp {
        case (_, messageSender, metricsSender, baseUrl) =>
          withMaterializer { implicit materializer =>
            val url = s"$baseUrl/ingests"

            val entity = createRequestWith(
              ingestType = "update"
            )

            whenPostRequestReady(url, entity) { response: HttpResponse =>
              response.status shouldBe StatusCodes.Created

              val ingestFuture =
                Unmarshal(response.entity).to[ResponseDisplayIngest]

              whenReady(ingestFuture) { actualIngest =>
                actualIngest.ingestType.id shouldBe "update"

                val payload =
                  messageSender.getMessages[SourceLocationPayload].head
                payload.context.ingestType shouldBe UpdateIngestType

                assertMetricSent(
                  metricsSender,
                  result = HttpMetricResults.Success)
              }
            }
          }
      }
    }

    describe("returns a 400 error for malformed requests") {
      it("if the ingest request doesn't have a sourceLocation") {
        withConfiguredApp {
          case (_, messageSender, metricsSender, baseUrl) =>
            withMaterializer { implicit materializer =>
              val url = s"$baseUrl/ingests"

              val entity = HttpEntity(
                ContentTypes.`application/json`,
                s"""|{
                   |  "type": "Ingest",
                   |  "ingestType": {
                   |    "id": "create",
                   |    "type": "IngestType"
                   |  },
                   |  "space": {
                   |    "id": "bcnfgh",
                   |    "type": "Space"
                   |  },
                   |  "externalIdentifier": "${createExternalIdentifier.underlying}"
                   |}""".stripMargin
              )

              whenPostRequestReady(url, entity) { response: HttpResponse =>
                assertIsUserErrorResponse(
                  response = response,
                  description =
                    "Invalid value at .sourceLocation: required property not supplied."
                )

                messageSender.messages shouldBe empty

                assertMetricSent(
                  metricsSender,
                  result = HttpMetricResults.UserError)
              }
            }
        }
      }

      it("if the body of the request is not valid JSON") {
        withConfiguredApp {
          case (_, messageSender, metricsSender, baseUrl) =>
            withMaterializer { implicit materialiser =>
              val url = s"$baseUrl/ingests"

              val entity = HttpEntity(
                ContentTypes.`application/json`,
                """hgjh""".stripMargin
              )

              whenPostRequestReady(url, entity) { response: HttpResponse =>
                assertIsUserErrorResponse(
                  response = response,
                  description =
                    "The request content was malformed:\nexpected json value got h (line 1, column 1)"
                )

                messageSender.messages shouldBe empty

                assertMetricSent(
                  metricsSender,
                  result = HttpMetricResults.UserError)
              }
            }
        }
      }

      it("if the Content-Type of the request is not an accepted type") {
        withConfiguredApp {
          case (_, messageSender, metricsSender, baseUrl) =>
            withMaterializer { implicit materialiser =>
              val url = s"$baseUrl/ingests"

              val entity = HttpEntity(
                ContentTypes.`text/plain(UTF-8)`,
                """hgjh""".stripMargin
              )

              whenPostRequestReady(url, entity) { response: HttpResponse =>
                assertIsUserErrorResponse(
                  response = response,
                  description =
                    "The request's Content-Type is not supported. Expected:\napplication/json",
                  statusCode = StatusCodes.UnsupportedMediaType,
                  label = "Unsupported Media Type"
                )

                messageSender.messages shouldBe empty

                assertMetricSent(
                  metricsSender,
                  result = HttpMetricResults.UserError)
              }
            }
        }
      }

      it("if the ingest request doesn't have a sourceLocation or ingestType") {
        withConfiguredApp {
          case (_, messageSender, metricsSender, baseUrl) =>
            withMaterializer { implicit materialiser =>
              val url = s"$baseUrl/ingests"

              val entity = HttpEntity(
                ContentTypes.`application/json`,
                s"""|{
                    |  "type": "Ingest",
                    |  "space": {
                    |    "id": "bcnfgh",
                    |    "type": "Space"
                    |  },
                    |  "externalIdentifier": "${createExternalIdentifier.underlying}"
                    |}""".stripMargin
              )

              whenPostRequestReady(url, entity) { response: HttpResponse =>
                assertIsUserErrorResponse(
                  response = response,
                  description =
                    """|Invalid value at .sourceLocation: required property not supplied.
                       |Invalid value at .ingestType: required property not supplied.""".stripMargin
                )

                messageSender.messages shouldBe empty

                assertMetricSent(
                  metricsSender,
                  result = HttpMetricResults.UserError)
              }
            }
        }
      }

      it("if the sourceLocation doesn't have a bucket field") {
        withConfiguredApp {
          case (_, messageSender, metricsSender, baseUrl) =>
            withMaterializer { implicit materialiser =>
              val url = s"$baseUrl/ingests"

              val entity = HttpEntity(
                ContentTypes.`application/json`,
                s"""|{
                    |  "type": "Ingest",
                    |  "ingestType": {
                    |    "id": "create",
                    |    "type": "IngestType"
                    |  },
                    |  "sourceLocation":{
                    |    "type": "Location",
                    |    "provider": {
                    |      "type": "Provider",
                    |      "id": "${StandardDisplayProvider.id}"
                    |    },
                    |    "path": "b22454408.zip"
                    |  },
                    |  "space": {
                    |    "id": "bcnfgh",
                    |    "type": "Space"
                    |  },
                    |  "externalIdentifier": "${createExternalIdentifier.underlying}"
                    |}""".stripMargin
              )

              whenPostRequestReady(url, entity) { response: HttpResponse =>
                assertIsUserErrorResponse(
                  response = response,
                  description =
                    "Invalid value at .sourceLocation.bucket: required property not supplied."
                )

                messageSender.messages shouldBe empty

                assertMetricSent(
                  metricsSender,
                  result = HttpMetricResults.UserError)
              }
            }
        }
      }

      it("if the request doesn't have an externalIdentifier") {
        withConfiguredApp {
          case (_, messageSender, metricsSender, baseUrl) =>
            withMaterializer { implicit materialiser =>
              val url = s"$baseUrl/ingests"

              val entity = HttpEntity(
                ContentTypes.`application/json`,
                s"""|{
                    |  "type": "Ingest",
                    |  "ingestType": {
                    |    "id": "create",
                    |    "type": "IngestType"
                    |  },
                    |  "sourceLocation":{
                    |    "type": "Location",
                    |    "provider": {
                    |      "type": "Provider",
                    |      "id": "${StandardDisplayProvider.id}"
                    |    },
                    |    "bucket": "${randomAlphanumeric()}",
                    |    "path": "b22454408.zip"
                    |  },
                    |  "space": {
                    |    "id": "bcnfgh",
                    |    "type": "Space"
                    |  }
                    |}""".stripMargin
              )

              whenPostRequestReady(url, entity) { response: HttpResponse =>
                assertIsUserErrorResponse(
                  response = response,
                  description =
                    "Invalid value at .externalIdentifier: required property not supplied."
                )

                messageSender.messages shouldBe empty

                assertMetricSent(
                  metricsSender,
                  result = HttpMetricResults.UserError)
              }
            }
        }
      }

      it("if the sourceLocation has an invalid bucket field") {
        withConfiguredApp {
          case (_, messageSender, metricsSender, baseUrl) =>
            withMaterializer { implicit materialiser =>
              val url = s"$baseUrl/ingests"

              val entity = HttpEntity(
                ContentTypes.`application/json`,
                s"""|{
                    |  "type": "Ingest",
                    |  "ingestType": {
                    |    "id": "create",
                    |    "type": "IngestType"
                    |  },
                    |  "sourceLocation":{
                    |    "type": "Location",
                    |    "provider": {
                    |      "type": "Provider",
                    |      "id": "${StandardDisplayProvider.id}"
                    |    },
                    |    "bucket": {"name": "bucket"},
                    |    "path": "b22454408.zip"
                    |  },
                    |  "space": {
                    |    "id": "bcnfgh",
                    |    "type": "Space"
                    |  },
                    |  "externalIdentifier": "${createExternalIdentifier.underlying}"
                    |}""".stripMargin
              )

              whenPostRequestReady(url, entity) { response: HttpResponse =>
                assertIsUserErrorResponse(
                  response = response,
                  description =
                    "Invalid value at .sourceLocation.bucket: should be a String."
                )

                messageSender.messages shouldBe empty

                assertMetricSent(
                  metricsSender,
                  result = HttpMetricResults.UserError)
              }
            }
        }
      }

      it("if the provider doesn't have a valid id field") {
        withConfiguredApp {
          case (_, messageSender, metricsSender, baseUrl) =>
            withMaterializer { implicit materialiser =>
              val url = s"$baseUrl/ingests"

              val entity = HttpEntity(
                ContentTypes.`application/json`,
                s"""|{
                    |  "type": "Ingest",
                    |  "ingestType": {
                    |    "id": "create",
                    |    "type": "IngestType"
                    |  },
                    |  "sourceLocation":{
                    |    "type": "Location",
                    |    "provider": {
                    |      "type": "Provider",
                    |      "id": "blipbloop"
                    |    },
                    |    "bucket": "bucket",
                    |    "path": "b22454408.zip"
                    |  },
                    |  "space": {
                    |    "id": "bcnfgh",
                    |    "type": "Space"
                    |  },
                    |  "externalIdentifier": "${createExternalIdentifier.underlying}"
                    |}""".stripMargin
              )

              whenPostRequestReady(url, entity) { response: HttpResponse =>
                assertIsUserErrorResponse(
                  response = response,
                  description =
                    """Invalid value at .sourceLocation.provider.id: got "blipbloop", valid values are: aws-s3-standard, aws-s3-ia."""
                )

                messageSender.messages shouldBe empty

                assertMetricSent(
                  metricsSender,
                  result = HttpMetricResults.UserError)
              }
            }
        }
      }

      it("if the ingestType doesn't have a valid id field") {
        withConfiguredApp {
          case (_, messageSender, metricsSender, baseUrl) =>
            withMaterializer { implicit materialiser =>
              val url = s"$baseUrl/ingests"

              val entity = createRequestWith(
                ingestType = "baboop"
              )

              whenPostRequestReady(url, entity) { response: HttpResponse =>
                assertIsUserErrorResponse(
                  response = response,
                  description =
                    """Invalid value at .ingestType.id: got "baboop", valid values are: create, update."""
                )

                messageSender.messages shouldBe empty

                assertMetricSent(
                  metricsSender,
                  result = HttpMetricResults.UserError)
              }
            }
        }
      }
    }

    it("returns a 500 Server Error if updating DynamoDB fails") {
      withMaterializer { implicit materializer =>
        withBrokenApp {
          case (_, _, metricsSender, baseUrl) =>
            val entity = HttpEntity(
              ContentTypes.`application/json`,
              s"""|{
                  |  "type": "Ingest",
                  |  "ingestType": {
                  |    "id": "create",
                  |    "type": "IngestType"
                  |  },
                  |  "sourceLocation":{
                  |    "type": "Location",
                  |    "provider": {
                  |      "type": "Provider",
                  |      "id": "${StandardDisplayProvider.id}"
                  |    },
                  |    "bucket": "bukkit",
                  |    "path": "key"
                  |  },
                  |  "space": {
                  |    "id": "space",
                  |    "type": "Space"
                  |  },
                  |  "callback": {
                  |    "url": "${testCallbackUri.toString}"
                  |  },
                  |  "externalIdentifier": "${createExternalIdentifier.underlying}"
                  |}""".stripMargin
            )

            whenPostRequestReady(s"$baseUrl/ingests/$randomUUID", entity) {
              response =>
                assertIsInternalServerErrorResponse(response)

                assertMetricSent(
                  metricsSender,
                  result = HttpMetricResults.ServerError)
            }
        }
      }
    }
  }

  describe("GET /ingests/find-by-bag-id/:bag-id") {
    it("returns a list of ingests for the given bag id") {
      withConfiguredApp {
        case (table, _, metricsSender, baseUrl) =>
          withMaterializer { implicit materialiser =>
            withIngestTracker(table) { ingestTracker =>
              val bagId = createBagId
              val ingest = createIngestWith(
                id = createIngestID,
                space = bagId.space,
                externalIdentifier = bagId.externalIdentifier
              )
              givenTableHasItem(ingest, table)

              whenGetRequestReady(s"$baseUrl/ingests/find-by-bag-id/$bagId") {
                response =>
                  response.status shouldBe StatusCodes.OK
                  response.entity.contentType shouldBe ContentTypes.`application/json`
                  val displayBagIngestFutures =
                    Unmarshal(
                      response.entity
                    ).to[List[DisplayIngestMinimal]]

                  whenReady(displayBagIngestFutures) { displayBagIngests =>
                    displayBagIngests shouldBe List(
                      DisplayIngestMinimal(ingest))
                  }

                  assertMetricSent(
                    metricsSender,
                    result = HttpMetricResults.Success
                  )
              }
            }
          }
      }
    }

    it("returns a list of ingests for the given bag id with : separated parts") {
      withConfiguredApp {
        case (table, _, metricsSender, baseUrl) =>
          withMaterializer { implicit materialiser =>
            withIngestTracker(table) { ingestTracker =>
              val bagId = createBagId
              val ingest = createIngestWith(
                id = createIngestID,
                space = bagId.space,
                externalIdentifier = bagId.externalIdentifier
              )
              givenTableHasItem(ingest, table)

              whenGetRequestReady(
                s"$baseUrl/ingests/find-by-bag-id/${bagId.space}:${bagId.externalIdentifier}") {
                response =>
                  response.status shouldBe StatusCodes.OK
                  response.entity.contentType shouldBe ContentTypes.`application/json`
                  val displayBagIngestFutures =
                    Unmarshal(response.entity).to[List[DisplayIngestMinimal]]

                  whenReady(displayBagIngestFutures) { displayBagIngests =>
                    displayBagIngests shouldBe List(
                      DisplayIngestMinimal(ingest))

                    assertMetricSent(
                      metricsSender,
                      result = HttpMetricResults.Success
                    )
                  }
              }
            }
          }
      }
    }

    it("returns 'Not Found' if there are no ingests for the given bag id") {
      withConfiguredApp {
        case (_, _, metricsSender, baseUrl) =>
          withMaterializer { implicit materialiser =>
            whenGetRequestReady(s"$baseUrl/ingests/find-by-bag-id/$randomUUID") {
              response =>
                response.status shouldBe StatusCodes.NotFound
                response.entity.contentType shouldBe ContentTypes.`application/json`
                val displayBagIngestFutures =
                  Unmarshal(response.entity).to[List[DisplayIngestMinimal]]
                whenReady(
                  displayBagIngestFutures
                ) { displayBagIngests =>
                  displayBagIngests shouldBe List.empty

                  assertMetricSent(
                    metricsSender,
                    result = HttpMetricResults.UserError
                  )

                }
            }
          }
      }
    }
  }
}
