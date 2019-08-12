package uk.ac.wellcome.platform.archive.bagreplicator.services

import org.scalatest.{FunSpec, Matchers}
import uk.ac.wellcome.platform.archive.common.generators.{
  ExternalIdentifierGenerators,
  StorageSpaceGenerators
}

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
}
