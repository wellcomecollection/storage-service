package uk.ac.wellcome.platform.archive.common.storage.services

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import uk.ac.wellcome.platform.archive.common.bagit.models.ExternalIdentifier
import uk.ac.wellcome.platform.archive.common.generators.{
  ExternalIdentifierGenerators,
  StorageSpaceGenerators
}
import uk.ac.wellcome.platform.archive.common.storage.models.StorageSpace

class DestinationBuilderTest
    extends AnyFunSpec
    with Matchers
    with ExternalIdentifierGenerators
    with StorageSpaceGenerators {

  it("constructs the correct path") {
    val storageSpace = createStorageSpace
    val externalIdentifier = createExternalIdentifier
    val version = createBagVersion

    val path = DestinationBuilder.buildPath(
      storageSpace = storageSpace,
      externalIdentifier = externalIdentifier,
      version = version
    )

    path shouldBe s"${storageSpace.underlying}/${externalIdentifier.toString}/$version"
  }

  it("uses slashes in the external identifier to build a hierarchy") {
    val version = createBagVersion
    val path = DestinationBuilder.buildPath(
      storageSpace = StorageSpace("alfa"),
      externalIdentifier = ExternalIdentifier("bravo/charlie"),
      version = version
    )

    path shouldBe s"alfa/bravo/charlie/$version"
  }
}
