package weco.storage_service.storage.services

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import weco.storage_service.bagit.models.ExternalIdentifier
import weco.storage_service.generators.{
  ExternalIdentifierGenerators,
  StorageSpaceGenerators
}
import weco.storage_service.storage.models.StorageSpace

class DestinationBuilderTest
    extends AnyFunSpec
    with Matchers
    with ExternalIdentifierGenerators
    with StorageSpaceGenerators {

  it("constructs the correct path") {
    val space = createStorageSpace
    val externalIdentifier = createExternalIdentifier
    val version = createBagVersion

    val path = DestinationBuilder.buildPath(
      space = space,
      externalIdentifier = externalIdentifier,
      version = version
    )

    path shouldBe s"${space.underlying}/${externalIdentifier.toString}/$version"
  }

  it("uses slashes in the external identifier to build a hierarchy") {
    val version = createBagVersion
    val path = DestinationBuilder.buildPath(
      space = StorageSpace("alfa"),
      externalIdentifier = ExternalIdentifier("bravo/charlie"),
      version = version
    )

    path shouldBe s"alfa/bravo/charlie/$version"
  }
}
