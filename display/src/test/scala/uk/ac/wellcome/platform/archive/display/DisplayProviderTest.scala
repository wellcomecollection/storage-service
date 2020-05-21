package uk.ac.wellcome.platform.archive.display

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.json.utils.JsonAssertions
import uk.ac.wellcome.platform.archive.common.ingests.models._

class DisplayProviderTest
    extends AnyFunSpec
    with Matchers
    with TableDrivenPropertyChecks
    with JsonAssertions {

  val providerPairs = Table(
    ("display", "internal"),
    (DisplayProvider("amazon-s3"), AmazonS3StorageProvider),
    (DisplayProvider("azure-blob-storage"), AzureBlobStorageProvider),
  )

  val deprecatedProviderPairs = Table(
    ("display", "internal"),
    (DisplayProvider("aws-s3-standard"), AmazonS3StorageProvider),
    (DisplayProvider("aws-s3-ia"), AmazonS3StorageProvider),
    (DisplayProvider("aws-s3-glacier"), AmazonS3StorageProvider),
  )

  it("turns a DisplayProvider into a StorageProvider") {
    forAll(providerPairs ++ deprecatedProviderPairs) {
      case (
          displayProvider: DisplayProvider,
          storageProvider: StorageProvider
          ) =>
        displayProvider.toStorageProvider shouldBe storageProvider
    }
  }

  it("turns a StorageProvider into a DisplayProvider") {
    forAll(providerPairs) {
      case (
          displayProvider: DisplayProvider,
          storageProvider: StorageProvider
          ) =>
        DisplayProvider(storageProvider) shouldBe displayProvider
    }
  }

  it("can't turn an invalid provider ID into a storage provider") {
    val badProvider = DisplayProvider(id = "not-a-storage-provider")

    val thrown = intercept[IllegalArgumentException] {
      badProvider.toStorageProvider
    }

    thrown.getMessage shouldBe (
      "Unrecognised storage provider ID: not-a-storage-provider; " +
        "valid values are: amazon-s3, azure-blob-storage"
    )
  }

  describe("JSON encoding/decoding") {
    val jsonPairs = Table(
      ("provider", "jsonString"),
      (
        DisplayProvider(AmazonS3StorageProvider),
        """{"id": "amazon-s3", "type": "Provider"}""",
      ),
      (
        DisplayProvider(AzureBlobStorageProvider),
        """{"id": "azure-blob-storage", "type": "Provider"}""",
      ),
    )

    it("decodes correctly") {
      forAll(jsonPairs) {
        case (displayProvider: DisplayProvider, jsonString: String) =>
          fromJson[DisplayProvider](jsonString).get shouldBe displayProvider
      }
    }

    it("encodes correctly") {
      forAll(jsonPairs) {
        case (displayProvider: DisplayProvider, jsonString: String) =>
          assertJsonStringsAreEqual(
            toJson[DisplayProvider](displayProvider).get,
            jsonString
          )
      }
    }

    val deprecatedJsonIds = Table(
      "json",
      """{"id": "aws-s3-standard", "type": "Provider"}""",
      """{"id": "aws-s3-ia", "type": "Provider"}""",
      """{"id": "aws-s3-glacier", "type": "Provider"}""",
    )

    it("decodes deprecated storage IDs") {
      forAll(deprecatedJsonIds) { jsonString =>
        fromJson[DisplayProvider](jsonString).get.toStorageProvider shouldBe AmazonS3StorageProvider
      }
    }
  }
}
