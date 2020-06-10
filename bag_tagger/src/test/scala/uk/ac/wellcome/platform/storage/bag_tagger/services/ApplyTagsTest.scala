package uk.ac.wellcome.platform.storage.bag_tagger.services

import org.scalatest.TryValues
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import uk.ac.wellcome.platform.archive.common.generators.StorageManifestGenerators
import uk.ac.wellcome.platform.archive.common.ingests.models.AmazonS3StorageProvider
import uk.ac.wellcome.platform.archive.common.storage.models.PrimaryStorageLocation
import uk.ac.wellcome.storage.fixtures.S3Fixtures
import uk.ac.wellcome.storage.tags.s3.S3Tags

import scala.util.Success

class ApplyTagsTest extends AnyFunSpec with Matchers with TryValues with S3Fixtures with StorageManifestGenerators {
  val s3Tags = new S3Tags()
  val applyTags = new ApplyTags(s3Tags = s3Tags)

  describe("it updates tags") {
    it("to objects in S3") {
      withLocalS3Bucket { bucket =>
        val prefix = createObjectLocationPrefixWith(bucket.name)

        val file = createStorageManifestFileWith(
          pathPrefix = "space/externalIdentifier/v1",
          name = "b1234.mxf"
        )

        val location = prefix.asLocation(file.path)

        s3Client.putObject(location.namespace, location.path, "<MXF file contents>")

        val result = applyTags.applyTags(
          storageLocations = Seq(
            PrimaryStorageLocation(
              provider = AmazonS3StorageProvider,
              prefix = prefix
            )
          ),
          tagsToApply = Map(file -> Map("Content-Type" -> "application/mxf"))
        )

        result shouldBe Success(())

        s3Tags.get(location).right.value shouldBe Map("Content-Type" -> "application/mxf")
      }
    }

    it("by appending to the existing tags") {
      true shouldBe false
    }
  }

  describe("it returns an error") {
    it("if asked to tag a non-existent object") {
      true shouldBe false
    }

    it("if asked to tag objects in Azure") {
      true shouldBe false
    }

    it("if tagging some of the objects fails") {
      true shouldBe false
    }

    it("if the objects have not been tagged by the verifier") {
      true shouldBe false
    }
  }
}
