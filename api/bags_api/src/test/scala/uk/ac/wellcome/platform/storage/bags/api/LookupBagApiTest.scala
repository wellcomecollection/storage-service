package uk.ac.wellcome.platform.storage.bags.api

import java.time.format.DateTimeFormatter

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.model.headers.{ETag, Location}
import io.circe.optics.JsonPath.root
import io.circe.parser.parse
import org.scalatest.{Assertion, FunSpec, Matchers}
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
import uk.ac.wellcome.storage.ObjectLocationPrefix

/** Tests for GET /bags/:space/:id.
  *
  */
class LookupBagApiTest
  extends FunSpec
    with Matchers
    with JsonAssertions
    with DisplayJsonHelpers
    with BagIdGenerators
    with StorageManifestGenerators
    with BagsApiFixture {

  it("finds the latest version of a bag") {
    val storageManifest = createStorageManifest

    withConfiguredApp(initialManifests = Seq(storageManifest)) {
      case (_, metrics, baseUrl) =>
        val url =
          s"$baseUrl/bags/${storageManifest.id.space.underlying}/${storageManifest.id.externalIdentifier.underlying}"

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

  it("can return very large responses") {
    // This is not an exact mechanism!
    // We can experiment to identify a size which exceeds the maxResponseByteLength
    val storageManifest = createStorageManifestWithFileCount(fileCount = 100)

    withLocalS3Bucket { bucket =>
      val prefix = ObjectLocationPrefix(bucket.name, randomAlphanumeric)

      withConfiguredApp(
        initialManifests = Seq(storageManifest),
        locationPrefix = prefix,
        maxResponseByteLength = 1000
      ) {
        case (_, metrics, baseUrl) =>
          val url =
            s"$baseUrl/bags/${storageManifest.id.space.underlying}/${storageManifest.id.externalIdentifier.underlying}"

          whenGetRequestReady(url) { response =>
            response.status shouldBe StatusCodes.TemporaryRedirect

            assertMetricSent(
              metrics,
              result = HttpMetricResults.Success
            )

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

  it("finds a specific version of a bag") {
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
            s"$baseUrl/bags/${storageSpace.underlying}/${externalIdentifier.underlying}?version=${storageManifest.version}"

          whenGetRequestReady(url) { response =>
            response.status shouldBe StatusCodes.OK

            withStringEntity(response.entity) {
              assertJsonMatches(_, storageManifest)
            }

            val header: ETag = response.header[ETag].get
            val etagValue = header.etag.value.replace("\"", "")

            etagValue shouldBe s"${storageManifest.space}/${storageManifest.info.externalIdentifier}/${storageManifest.version}"

            assertMetricSent(
              metrics,
              result = HttpMetricResults.Success
            )
          }
        }
    }
  }

  val storageManifestWithSlash: StorageManifest = createStorageManifestWith(
    bagInfo = createBagInfoWith(
      externalIdentifier = ExternalIdentifier("alfa/bravo")
    )
  )

  it("finds a bag with a slash in the external identifier (URL-encoded)") {
    withConfiguredApp(initialManifests = Seq(storageManifestWithSlash)) {
      case (_, _, baseUrl) =>
        val url = s"$baseUrl/bags/${storageManifestWithSlash.id.space.underlying}/alfa%2Fbravo"

        whenGetRequestReady(url) { response =>
          response.status shouldBe StatusCodes.OK

          withStringEntity(response.entity) {
            assertJsonMatches(_, storageManifestWithSlash)
          }
        }
    }
  }

  it("finds a bag with a slash in the external identifier (not URL-encoded)") {
    withConfiguredApp(initialManifests = Seq(storageManifestWithSlash)) {
      case (_, _, baseUrl) =>
        val url = s"$baseUrl/bags/${storageManifestWithSlash.id.space.underlying}/alfa/bravo"

        whenGetRequestReady(url) { response =>
          response.status shouldBe StatusCodes.OK

          withStringEntity(response.entity) {
            assertJsonMatches(_, storageManifestWithSlash)
          }
        }
    }
  }

  // Creating a bag whose external identifier ends with /versions seems unlikely
  // in practice, but we should be able to support it if you correctly URL-encode
  // the slash.
  //
  // (Do not create bags like this.  It is silly.)
  it("finds a bag whose identifier ends with /versions") {
    val storageManifest = createStorageManifestWith(
      bagInfo = createBagInfoWith(
        externalIdentifier = ExternalIdentifier("alfa/versions")
      )
    )

    withConfiguredApp(initialManifests = Seq(storageManifest)) {
      case (_, _, baseUrl) =>
        val url = s"$baseUrl/bags/${storageManifest.id.space.underlying}/alfa%2Fversions"

        whenGetRequestReady(url) { response =>
          response.status shouldBe StatusCodes.OK

          withStringEntity(response.entity) {
            assertJsonMatches(_, storageManifest)
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
          s"$baseUrl/bags/${storageManifest.id.space.underlying}/${storageManifest.id.externalIdentifier.underlying}"
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
    it("if there are no manifests for this bag ID") {
      withConfiguredApp() {
        case (_, metrics, baseUrl) =>
          val bagId = createBagId
          whenGetRequestReady(
            s"$baseUrl/bags/${bagId.space}/${bagId.externalIdentifier}"
          ) { response =>
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

    it("if you ask for a bag ID in the wrong space") {
      val storageManifest = createStorageManifest

      withConfiguredApp(initialManifests = Seq(storageManifest)) {
        case (_, metrics, baseUrl) =>
          val badId =
            s"${storageManifest.space}123/${storageManifest.id.externalIdentifier}"
          whenGetRequestReady(s"$baseUrl/bags/$badId") { response =>
            assertIsUserErrorResponse(
              response,
              description = s"Storage manifest $badId not found",
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
            s"$baseUrl/bags/${storageManifest.space}/${storageManifest.id.externalIdentifier}?version=${storageManifest.version.increment}"
          ) { response =>
            assertIsUserErrorResponse(
              response,
              description =
                s"Storage manifest ${storageManifest.id} version ${storageManifest.version.increment} not found",
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
                description =
                  s"Storage manifest $bagId version $badVersion not found",
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
