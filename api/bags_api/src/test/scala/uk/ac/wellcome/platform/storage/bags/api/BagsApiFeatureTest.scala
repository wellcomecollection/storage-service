package uk.ac.wellcome.platform.storage.bags.api

import java.time.format.DateTimeFormatter

import akka.http.scaladsl.model._
import io.circe.optics.JsonPath._
import io.circe.parser._
import org.scalatest.concurrent.{IntegrationPatience, ScalaFutures}
import org.scalatest.{FunSpec, Matchers}
import uk.ac.wellcome.json.utils.JsonAssertions
import uk.ac.wellcome.platform.archive.common.generators.{
  BagIdGenerators,
  BagInfoGenerators,
  StorageManifestGenerators
}
import uk.ac.wellcome.platform.archive.common.http.HttpMetricResults
import uk.ac.wellcome.platform.archive.common.storage.models.StorageManifest
import uk.ac.wellcome.platform.archive.display.fixtures.DisplayJsonHelpers
import uk.ac.wellcome.platform.storage.bags.api.fixtures.BagsApiFixture

class BagsApiFeatureTest
    extends FunSpec
    with Matchers
    with ScalaFutures
    with BagIdGenerators
    with BagInfoGenerators
    with BagsApiFixture
    with IntegrationPatience
    with StorageManifestGenerators
    with DisplayJsonHelpers
    with JsonAssertions {

  describe("GET /bags/:space/:id") {
    it("returns a bag when available") {
      withConfiguredApp {
        case (vhs, metricsSender, baseUrl) =>
          withMaterializer { implicit materializer =>
            val storageManifest: StorageManifest = createStorageManifestWith(
              locations = List(
                createObjectLocation,
                createObjectLocation,
                createObjectLocation
              )
            )

            val expectedJson =
              s"""
                 |{
                 |  "@context": "http://api.wellcomecollection.org/storage/v1/context.json",
                 |  "id": "${storageManifest.id.toString}",
                 |  "space": {
                 |    "id": "${storageManifest.space.underlying}",
                 |    "type": "Space"
                 |  },
                 |  "info": ${bagInfo(storageManifest.info)},
                 |  "manifest": ${manifest(storageManifest.manifest)},
                 |  "tagManifest": ${manifest(storageManifest.tagManifest)},
                 |  "locations": [
                 |    ${asList(storageManifest.locations, location)}
                 |  ],
                 |  "createdDate": "${DateTimeFormatter.ISO_INSTANT.format(
                   storageManifest.createdDate)}",
                 |  "type": "Bag"
                 |}
               """.stripMargin

            storeSingleManifest(vhs, storageManifest)
            val url =
              s"$baseUrl/bags/${storageManifest.id.space.underlying}/${storageManifest.id.externalIdentifier.underlying}"

            whenGetRequestReady(url) { response =>
              response.status shouldBe StatusCodes.OK

              withStringEntity(response.entity) { actualJson =>
                assertJsonStringsAreEqual(actualJson, expectedJson)
              }

              assertMetricSent(
                metricsSender,
                result = HttpMetricResults.Success
              )
            }
          }
      }
    }

    it("does not output null values") {
      withConfiguredApp {
        case (vhs, metricsSender, baseUrl) =>
          withMaterializer { implicit materializer =>
            val storageManifest = createStorageManifestWith(
              bagInfo = createBagInfoWith(externalDescription = None)
            )
            storeSingleManifest(vhs, storageManifest)

            whenGetRequestReady(
              s"$baseUrl/bags/${storageManifest.id.space.underlying}/${storageManifest.id.externalIdentifier.underlying}") {
              response =>
                response.status shouldBe StatusCodes.OK

                withStringEntity(response.entity) { jsonString =>
                  val infoJson =
                    root.info.json
                      .getOption(parse(jsonString).right.get)
                      .get
                  infoJson.findAllByKey("externalDescription") shouldBe empty
                }

                assertMetricSent(
                  metricsSender,
                  result = HttpMetricResults.Success)
            }
          }
      }
    }

    it("returns a 404 NotFound if no ingest monitor matches id") {
      withMaterializer { implicit materializer =>
        withConfiguredApp {
          case (_, metricsSender, baseUrl) =>
            val bagId = createBagId
            whenGetRequestReady(
              s"$baseUrl/bags/${bagId.space}/${bagId.externalIdentifier}") {
              response =>
                assertIsUserErrorResponse(
                  response,
                  description = s"Storage manifest $bagId not found",
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

    it("returns a 500 error if looking up the bag fails") {
      withMaterializer { implicit materializer =>
        withBrokenApp {
          case (_, metricsSender, baseUrl) =>
            val bagId = createBagId
            whenGetRequestReady(
              s"$baseUrl/bags/${bagId.space}/${bagId.externalIdentifier}") {
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
}
