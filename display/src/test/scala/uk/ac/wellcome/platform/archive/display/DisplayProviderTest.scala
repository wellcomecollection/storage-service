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
    (DisplayProvider("aws-s3-standard"), StandardStorageProvider),
    (DisplayProvider("aws-s3-ia"), InfrequentAccessStorageProvider),
    (DisplayProvider("aws-s3-glacier"), GlacierStorageProvider)
  )

  it("turns a DisplayProvider into a StorageProvider") {
    forAll(providerPairs) {
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
        "valid values are: aws-s3-standard, aws-s3-ia, aws-s3-glacier"
      )
  }

  describe("JSON encoding/decoding") {
    val jsonPairs = Table(
      ("provider", "json"),
      (
        DisplayProvider("aws-s3-standard"),
        """{"id": "aws-s3-standard", "type": "Provider"}"""
      ),
      (
        DisplayProvider("aws-s3-ia"),
        """{"id": "aws-s3-ia", "type": "Provider"}"""
      ),
      (
        DisplayProvider("aws-s3-glacier"),
        """{"id": "aws-s3-glacier", "type": "Provider"}"""
      )
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
  }
}
