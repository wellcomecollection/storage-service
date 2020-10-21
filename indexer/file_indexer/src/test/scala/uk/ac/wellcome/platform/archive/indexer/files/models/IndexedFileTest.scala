package uk.ac.wellcome.platform.archive.indexer.files.models

import java.time.Instant

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import uk.ac.wellcome.platform.archive.common.generators.StorageManifestGenerators
import uk.ac.wellcome.platform.archive.common.storage.models.{PrimaryS3StorageLocation, StorageManifestFile}
import uk.ac.wellcome.platform.archive.common.verify.{SHA256, SHA512}
import uk.ac.wellcome.platform.archive.indexer.elasticsearch.models.FileContext
import uk.ac.wellcome.storage.s3.{S3ObjectLocation, S3ObjectLocationPrefix}

class IndexedFileTest extends AnyFunSpec with Matchers with StorageManifestGenerators {
  it("creates an IndexedFile from a FileContext") {
    val context = FileContext(
      space = createStorageSpace,
      externalIdentifier = createExternalIdentifier,
      hashingAlgorithm = chooseFrom(SHA256, SHA512),
      bagLocation = PrimaryS3StorageLocation(
        prefix = createS3ObjectLocationPrefix
      ),
      file = createStorageManifestFile,
      createdDate = Instant.now
    )

    val indexedFile = IndexedFile(context)

    indexedFile.space shouldBe context.space
    indexedFile.externalIdentifier shouldBe context.externalIdentifier
    context.file.name should endWith(indexedFile.suffix)
    indexedFile.size shouldBe context.file.size
    indexedFile.location shouldBe context.bagLocation.prefix.asLocation(context.file.path)
    indexedFile.createdDate shouldBe context.createdDate

    indexedFile.checksum shouldBe IndexedChecksum(
      algorithm = context.hashingAlgorithm.toString,
      value = context.file.checksum.toString
    )
  }

  it("uses the correct versioned path") {
    val file = StorageManifestFile(
      checksum = createChecksum.value,
      name = "bag-info.txt",
      path = "v1/bag-info.txt",
      size = 100
    )

    val context = FileContext(
      space = createStorageSpace,
      externalIdentifier = createExternalIdentifier,
      hashingAlgorithm = chooseFrom(SHA256, SHA512),
      bagLocation = PrimaryS3StorageLocation(
        prefix = S3ObjectLocationPrefix(
          bucket = "example-storage",
          keyPrefix = "example/b1234"
        )
      ),
      file = file,
      createdDate = Instant.now
    )

    val indexedFile = IndexedFile(context)

    indexedFile.location shouldBe S3ObjectLocation(
      bucket = "example-storage",
      key = "example/b1234/v1/bag-info.txt"
    )
  }
}
