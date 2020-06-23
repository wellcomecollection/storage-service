package uk.ac.wellcome.platform.storage.bags.api

import java.time.format.DateTimeFormatter

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.model.headers.{ETag, Location}
import io.circe.optics.JsonPath.root
import io.circe.parser.parse
import org.scalatest.Assertion
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import uk.ac.wellcome.json.utils.JsonAssertions
import uk.ac.wellcome.platform.archive.common.bagit.models.{
  BagVersion,
  ExternalIdentifier
}
import uk.ac.wellcome.platform.archive.common.generators.{
  BagIdGenerators,
  StorageManifestGenerators
}
import uk.ac.wellcome.platform.archive.common.http.HttpMetricResults
import uk.ac.wellcome.platform.archive.common.storage.models.StorageManifest
import uk.ac.wellcome.platform.archive.display.fixtures.DisplayJsonHelpers
import uk.ac.wellcome.platform.storage.bags.api.fixtures.BagsApiFixture

/** Tests for GET /bags/:space/:id.
  *
  */
class LookupBagApiTest
    extends AnyFunSpec
    with Matchers
    with JsonAssertions
    with DisplayJsonHelpers
    with BagIdGenerators
    with StorageManifestGenerators
    with BagsApiFixture {

  describe("finding the latest version of a bag") {
    it("returns the latest version") {
      val storageManifest = createStorageManifest

      withConfiguredApp(initialManifests = Seq(storageManifest)) {
        case (_, _, baseUrl) =>
          val url = s"$baseUrl/bags/${storageManifest.id}"

          whenGetRequestReady(url) { response =>
            withStringEntity(response.entity) {
              assertJsonMatches(_, storageManifest)
            }
          }
      }
    }

    it("returns a 404 if there are no versions of this bag") {
      val bagId = createBagId

      withConfiguredApp() {
        case (_, metrics, baseUrl) =>
          whenGetRequestReady(s"$baseUrl/bags/$bagId") { response =>
            assertIsUserErrorResponse(
              response,
              description = s"Storage manifest $bagId not found",
              statusCode = StatusCodes.NotFound,
              label = "Not Found"
            )

            assertMetricSent(metrics, result = HttpMetricResults.UserError)
          }
      }
    }

    it("returns a 500 if looking up the latest version fails") {
      withBrokenApp {
        case (metrics, baseUrl) =>
          whenGetRequestReady(s"$baseUrl/bags/$createBagId") { response =>
            assertIsInternalServerErrorResponse(response)

            assertMetricSent(metrics, result = HttpMetricResults.ServerError)
          }
      }
    }
  }

  describe("looking up an exact version of a bag") {
    it("returns the bag") {
      val storageManifest = createStorageManifest

      withConfiguredApp(initialManifests = Seq(storageManifest)) {
        case (_, metrics, baseUrl) =>
          val url =
            s"$baseUrl/bags/${storageManifest.id}?version=${storageManifest.version}"

          whenGetRequestReady(url) { response =>
            response.status shouldBe StatusCodes.OK

            withStringEntity(response.entity) {
              assertJsonMatches(_, storageManifest)
            }

            val header: ETag = response.header[ETag].get
            val etagValue = header.etag.value.replace("\"", "")

            etagValue shouldBe storageManifest.idWithVersion

            assertMetricSent(
              metrics,
              result = HttpMetricResults.Success
            )
          }
      }
    }

    it("returns large responses") {
      // This is not an exact mechanism!
      // We can experiment to identify a size which exceeds the maxResponseByteLength
      val storageManifest = createStorageManifestWithFileCount(fileCount = 100)

      withLocalS3Bucket { bucket =>
        withConfiguredApp(
          initialManifests = Seq(storageManifest),
          locationPrefix = createS3ObjectLocationPrefixWith(bucket),
          maxResponseByteLength = 1000
        ) {
          case (_, metrics, baseUrl) =>
            val url =
              s"$baseUrl/bags/${storageManifest.id}?version=${storageManifest.version}"

            whenGetRequestReady(url) { response =>
              response.status shouldBe StatusCodes.TemporaryRedirect

              assertMetricSent(metrics, result = HttpMetricResults.Success)

              val redirectedUrl = response.header[Location].get.uri.toString()

              whenGetRequestReady(redirectedUrl) { redirectedResponse =>
                withStringEntity(redirectedResponse.entity) {
                  assertJsonMatches(_, storageManifest)
                }
              }
            }
        }
      }
    }

    it("can return cached responses from previous requests") {
      val storageManifest = createStorageManifestWithFileCount(fileCount = 100)

      // TODO: Ideally this test would do some introspection on the bucket
      // or the dao, and see that we only did a lookup on the first request.
      //
      // For now, this test at least checks the path (cached response exists)
      // works correctly.

      withLocalS3Bucket { bucket =>
        withConfiguredApp(
          initialManifests = Seq(storageManifest),
          locationPrefix = createS3ObjectLocationPrefixWith(bucket),
          maxResponseByteLength = 1000
        ) {
          case (_, metrics, baseUrl) =>
            val url =
              s"$baseUrl/bags/${storageManifest.id}?version=${storageManifest.version}"

            (1 to 3).foreach { _ =>
              whenGetRequestReady(url) { response =>
                response.status shouldBe StatusCodes.TemporaryRedirect

                assertMetricSent(metrics, result = HttpMetricResults.Success)

                val redirectedUrl = response.header[Location].get.uri.toString()

                whenGetRequestReady(redirectedUrl) { redirectedResponse =>
                  withStringEntity(redirectedResponse.entity) {
                    assertJsonMatches(_, storageManifest)
                  }
                }
              }
            }
        }
      }
    }

    it("finds specific versions") {
      val externalIdentifier = createExternalIdentifier
      val storageSpace = createStorageSpace

      val manifests = (1 to 5).map { version =>
        createStorageManifestWith(
          bagInfo = createBagInfoWith(
            externalIdentifier = externalIdentifier
          ),
          space = storageSpace,
          version = BagVersion(version)
        )
      }

      withConfiguredApp(initialManifests = manifests) {
        case (_, metrics, baseUrl) =>
          manifests.foreach { storageManifest =>
            val url =
              s"$baseUrl/bags/$storageSpace/$externalIdentifier?version=${storageManifest.version}"

            whenGetRequestReady(url) { response =>
              response.status shouldBe StatusCodes.OK

              withStringEntity(response.entity) {
                assertJsonMatches(_, storageManifest)
              }

              val header: ETag = response.header[ETag].get
              val etagValue = header.etag.value.replace("\"", "")

              etagValue shouldBe s"$storageSpace/$externalIdentifier/${storageManifest.version}"

              assertMetricSent(
                metrics,
                result = HttpMetricResults.Success
              )
            }
          }
      }
    }
  }

  describe("looking up a bag with a slash in the external identifier") {
    val storageManifestWithSlash: StorageManifest = createStorageManifestWith(
      bagInfo = createBagInfoWith(
        externalIdentifier = ExternalIdentifier("alfa/bravo")
      )
    )

    it("when the identifier is URL encoded") {
      withConfiguredApp(initialManifests = Seq(storageManifestWithSlash)) {
        case (_, _, baseUrl) =>
          val url =
            s"$baseUrl/bags/${storageManifestWithSlash.id.space}/alfa%2Fbravo?version=${storageManifestWithSlash.version}"

          whenGetRequestReady(url) { response =>
            response.status shouldBe StatusCodes.OK

            withStringEntity(response.entity) {
              assertJsonMatches(_, storageManifestWithSlash)
            }
          }
      }
    }

    it("when the identifier is not URL-encoded") {
      withConfiguredApp(initialManifests = Seq(storageManifestWithSlash)) {
        case (_, _, baseUrl) =>
          val url =
            s"$baseUrl/bags/${storageManifestWithSlash.id.space}/alfa/bravo?version=${storageManifestWithSlash.version}"

          whenGetRequestReady(url) { response =>
            response.status shouldBe StatusCodes.OK

            withStringEntity(response.entity) {
              assertJsonMatches(_, storageManifestWithSlash)
            }
          }
      }
    }
  }

  it("does not output null values") {
    val storageManifest = createStorageManifestWith(
      bagInfo = createBagInfoWith(externalDescription = None)
    )

    withConfiguredApp(initialManifests = Seq(storageManifest)) {
      case (_, _, baseUrl) =>
        whenGetRequestReady(
          s"$baseUrl/bags/${storageManifest.id}?version=${storageManifest.version}"
        ) { response =>
          response.status shouldBe StatusCodes.OK

          withStringEntity(response.entity) { jsonString =>
            val infoJson =
              root.info.json
                .getOption(parse(jsonString).right.get)
                .get
            infoJson.findAllByKey("externalDescription") shouldBe empty
          }
        }
    }
  }

  describe("returns a 404 Not Found for missing bags") {
    it("if you ask for a bag ID in the wrong space") {
      val storageManifest = createStorageManifest
      val badId =
        s"${storageManifest.space}123/${storageManifest.id.externalIdentifier}"

      withConfiguredApp(initialManifests = Seq(storageManifest)) {
        case (_, metrics, baseUrl) =>
          whenGetRequestReady(
            s"$baseUrl/bags/$badId?version=${storageManifest.version}"
          ) { response =>
            assertIsUserErrorResponse(
              response,
              description =
                s"Storage manifest $badId ${storageManifest.version} not found",
              statusCode = StatusCodes.NotFound,
              label = "Not Found"
            )

            assertMetricSent(metrics, result = HttpMetricResults.UserError)
          }
      }
    }

    it("if you ask for a version that doesn't exist") {
      val storageManifest = createStorageManifest

      withConfiguredApp(initialManifests = Seq(storageManifest)) {
        case (_, metrics, baseUrl) =>
          whenGetRequestReady(
            s"$baseUrl/bags/${storageManifest.id}?version=${storageManifest.version.increment}"
          ) { response =>
            assertIsUserErrorResponse(
              response,
              description =
                s"Storage manifest ${storageManifest.id} ${storageManifest.version.increment} not found",
              statusCode = StatusCodes.NotFound,
              label = "Not Found"
            )

            assertMetricSent(metrics, result = HttpMetricResults.UserError)
          }
      }
    }

    it("if you ask for a non-numeric version") {
      val badVersion = randomAlphanumeric

      val bagId = createBagId

      withConfiguredApp() {
        case (_, metrics, baseUrl) =>
          whenGetRequestReady(s"$baseUrl/bags/$bagId?version=$badVersion") {
            response =>
              assertIsUserErrorResponse(
                response,
                description = s"Storage manifest $bagId $badVersion not found",
                statusCode = StatusCodes.NotFound,
                label = "Not Found"
              )

              assertMetricSent(metrics, result = HttpMetricResults.UserError)
          }
      }
    }
  }

  it("returns a 500 error if looking up the bag fails") {
    withBrokenApp {
      case (metrics, baseUrl) =>
        whenGetRequestReady(s"$baseUrl/bags/$createBagId") { response =>
          assertIsInternalServerErrorResponse(response)

          assertMetricSent(metrics, result = HttpMetricResults.ServerError)
        }
    }
  }

  it("returns a 500 error if looking up a specific version of a bag fails") {
    withBrokenApp {
      case (metrics, baseUrl) =>
        whenGetRequestReady(s"$baseUrl/bags/$createBagId?version=v1") {
          response =>
            assertIsInternalServerErrorResponse(response)

            assertMetricSent(metrics, result = HttpMetricResults.ServerError)
        }
    }
  }

  private def assertJsonMatches(
    json: String,
    storageManifest: StorageManifest
  ): Assertion = {
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
         |  "location": ${location(storageManifest.location)},
         |  "replicaLocations": [
         |    ${asList(storageManifest.replicaLocations, location)}
         |  ],
         |  "createdDate": "${DateTimeFormatter.ISO_INSTANT.format(
           storageManifest.createdDate
         )}",
         |  "version": "${storageManifest.version}",
         |  "type": "Bag"
         |}
       """.stripMargin

    assertJsonStringsAreEqual(json, expectedJson)
  }
}
