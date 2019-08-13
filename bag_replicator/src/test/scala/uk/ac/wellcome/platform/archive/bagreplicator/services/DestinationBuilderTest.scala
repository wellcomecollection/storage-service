package uk.ac.wellcome.platform.archive.bagreplicator.services

import org.scalatest.{FunSpec, Matchers}
import uk.ac.wellcome.platform.archive.common.bagit.models.ExternalIdentifier
import uk.ac.wellcome.platform.archive.common.generators.{
  ExternalIdentifierGenerators,
  StorageSpaceGenerators
}
import uk.ac.wellcome.platform.archive.common.storage.models.StorageSpace

class DestinationBuilderTest
    extends FunSpec
    with Matchers
    with ExternalIdentifierGenerators
    with StorageSpaceGenerators {

  def createNamespace: String =
    randomAlphanumeric

  it("uses the given namespace") {
    val namespace = createNamespace

    val builder = new DestinationBuilder(
      namespace = namespace
    )

    val location = builder.buildDestination(
      storageSpace = createStorageSpace,
      externalIdentifier = createExternalIdentifier,
      version = createBagVersion
    )

    location.namespace shouldBe namespace
  }

  it("constructs the correct path") {
    val builder = new DestinationBuilder(
      namespace = createNamespace
    )

    val storageSpace = createStorageSpace
    val externalIdentifier = createExternalIdentifier
    val version = createBagVersion

    val location = builder.buildDestination(
      storageSpace = storageSpace,
      externalIdentifier = externalIdentifier,
      version = version
    )

    location.path shouldBe s"${storageSpace.underlying}/${externalIdentifier.toString}/$version"
  }

  it("URL encodes slashes in the path") {
    val builder = new DestinationBuilder(
      namespace = createNamespace
    )

    val version = createBagVersion

    val externalIdentifier = createExternalIdentifier

    val location = builder.buildDestination(
      storageSpace = StorageSpace("a/b"),
      externalIdentifier = externalIdentifier,
      version = version
    )

    location.path shouldBe s"a%2Fb/$externalIdentifier/$version"
  }

  it("encodes slashes in a way that avoids ambiguity") {
    val builder = new DestinationBuilder(
      namespace = createNamespace
    )

    val version = createBagVersion

    val component1 = s"1-$randomAlphanumeric"
    val component2 = s"2-$randomAlphanumeric"
    val component3 = s"3-$randomAlphanumeric"

    val location_12_3 = builder.buildDestination(
      storageSpace = StorageSpace(s"$component1/$component2"),
      externalIdentifier = ExternalIdentifier(component3),
      version = version
    )

    val location_1_23 = builder.buildDestination(
      storageSpace = StorageSpace(component1),
      externalIdentifier = ExternalIdentifier(s"$component2/$component3"),
      version = version
    )

    location_12_3 should not be location_1_23
  }
}
