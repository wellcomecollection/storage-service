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

  def createNamespace: String =
    randomAlphanumeric

  it("uses the given namespace") {
    val namespace = createNamespace

    val location = DestinationBuilder.buildDestination(
      namespace = namespace,
      createStorageSpace,
      createExternalIdentifier,
      createBagVersion
    )

    location.namespace shouldBe namespace
  }

  it("constructs the correct path") {
    val namespace = createNamespace

    val storageSpace = createStorageSpace
    val externalIdentifier = createExternalIdentifier
    val version = createBagVersion

    val location = DestinationBuilder.buildDestination(
      namespace = namespace,
      storageSpace,
      externalIdentifier,
      version
    )

    location.path shouldBe s"${storageSpace.underlying}/${externalIdentifier.toString}/$version"
  }

  it("uses slashes in the external identifier to build a hierarchy") {
    val namespace = createNamespace
    val version = createBagVersion
    val location = DestinationBuilder.buildDestination(
      namespace = namespace,
      StorageSpace("alfa"),
      ExternalIdentifier("bravo/charlie"),
      version
    )

    location.path shouldBe s"alfa/bravo/charlie/$version"
  }
}
