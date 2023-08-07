package weco.storage_service.storage.models

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import weco.storage.providers.azure.AzureBlobLocationPrefix
import weco.storage.generators.{
  AzureBlobLocationGenerators,
  S3ObjectLocationGenerators
}
import weco.storage.providers.s3.S3ObjectLocationPrefix

class EnsureTrailingSlashTest
    extends AnyFunSpec
    with Matchers
    with S3ObjectLocationGenerators
    with AzureBlobLocationGenerators {
  import EnsureTrailingSlash._

  it("it does not add a slash to an empty path") {
    val prefix =
      S3ObjectLocationPrefix(bucket = createBucketName, keyPrefix = "")
    prefix.withTrailingSlash shouldBe prefix
  }

  it("adds a slash to a path without one") {
    val prefix = S3ObjectLocationPrefix(
      bucket = createBucketName,
      keyPrefix = "path/to/stuff"
    )
    prefix.withTrailingSlash shouldBe prefix.copy(keyPrefix = "path/to/stuff/")
  }

  it("does not add slash to a path that already has one") {
    val prefix = S3ObjectLocationPrefix(
      bucket = createBucketName,
      keyPrefix = "path/with/trailing/slash/"
    )
    prefix.withTrailingSlash shouldBe prefix.copy(
      keyPrefix = "path/with/trailing/slash/"
    )
  }

  it("adds slashes to Azure prefixes") {
    val prefix = AzureBlobLocationPrefix(
      container = createContainerName,
      namePrefix = "path/to/stuff"
    )
    prefix.withTrailingSlash shouldBe prefix.copy(namePrefix = "path/to/stuff/")
  }
}
