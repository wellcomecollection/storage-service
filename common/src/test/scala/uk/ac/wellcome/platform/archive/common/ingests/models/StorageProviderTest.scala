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
    ("aws-s3-standard", AmazonS3StorageProvider),
    ("aws-s3-ia", AmazonS3StorageProvider),
    ("aws-s3-glacier", AmazonS3StorageProvider),
    ("amazon-s3", AmazonS3StorageProvider),
    ("azure-blob-storage", AzureBlobStorageProvider),
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
        "valid values are: amazon-s3, azure-blob-storage"
    )
  }
}
