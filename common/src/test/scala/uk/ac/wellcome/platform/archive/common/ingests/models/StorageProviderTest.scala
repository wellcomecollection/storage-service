package uk.ac.wellcome.platform.archive.common.ingests.models

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks

class StorageProviderTest
    extends AnyFunSpec
    with Matchers
    with TableDrivenPropertyChecks {
  val providerPairs = Table(
    ("id", "provider"),
    ("aws-s3-standard", StandardStorageProvider),
    ("aws-s3-ia", InfrequentAccessStorageProvider),
    ("aws-s3-glacier", GlacierStorageProvider)
  )

  it("creates the correct storage provider from an ID") {
    forAll(providerPairs) {
      case (id: String, storageProvider: StorageProvider) =>
        StorageProvider(id) shouldBe storageProvider
    }
  }

  it("throws an error if it gets an unrecognised ID") {
    val thrown = intercept[IllegalArgumentException] {
      StorageProvider("not-a-storage-provider")
    }

    thrown.getMessage shouldBe (
      "Unrecognised storage provider ID: not-a-storage-provider; " +
        "valid values are: aws-s3-standard, aws-s3-ia, aws-s3-glacier"
    )
  }
}
