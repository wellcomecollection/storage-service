package uk.ac.wellcome.platform.archive.bagreplicator.fixtures

import com.amazonaws.services.s3.model.PutObjectResult
import org.scalatest.Assertion
import uk.ac.wellcome.platform.archive.common.fixtures.RandomThings
import uk.ac.wellcome.storage.ObjectLocation
import uk.ac.wellcome.storage.fixtures.S3
import uk.ac.wellcome.storage.fixtures.S3.Bucket

trait S3CopierFixtures extends S3 with RandomThings {
  def createObjectLocationWith(
    bucket: Bucket = Bucket(randomAlphanumeric()),
    key: String = randomAlphanumeric()
  ): ObjectLocation =
    ObjectLocation(
      namespace = bucket.name,
      key = key
    )

  def createObjectLocation: ObjectLocation = createObjectLocationWith()

  def createObject(location: ObjectLocation,
                   content: String = randomAlphanumeric()): PutObjectResult =
    s3Client.putObject(
      location.namespace,
      location.key,
      content
    )

  def assertEqualObjects(x: ObjectLocation, y: ObjectLocation): Assertion =
    getContentFromS3(x) shouldBe getContentFromS3(y)
}
