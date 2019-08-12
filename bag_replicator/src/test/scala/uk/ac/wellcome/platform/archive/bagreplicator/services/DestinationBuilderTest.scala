package uk.ac.wellcome.platform.archive.bagreplicator.services

import org.scalatest.{FunSpec, Matchers}
import uk.ac.wellcome.platform.archive.common.bagit.models.BagVersion
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
      namespace = namespace,
      rootPath = None
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
      namespace = createNamespace,
      rootPath = None
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

  it("uses the root path if provided") {
    val builder = new DestinationBuilder(
      namespace = "MyNamespace",
      rootPath = Some("RootPath")
    )

    val storageSpace = createStorageSpace
    val externalIdentifier = createExternalIdentifier

    val location = builder.buildDestination(
      storageSpace = storageSpace,
      externalIdentifier = externalIdentifier,
      version = BagVersion(1)
    )

    location.namespace shouldBe "MyNamespace"
    location.path shouldBe s"RootPath/${storageSpace.underlying}/${externalIdentifier.toString}/v1"
  }

  it("skips the root path if not provided") {
    val builder = new DestinationBuilder(
      namespace = "MyNamespace",
      rootPath = None
    )

    val storageSpace = createStorageSpace
    val externalIdentifier = createExternalIdentifier

    val location = builder.buildDestination(
      storageSpace = storageSpace,
      externalIdentifier = externalIdentifier,
      version = BagVersion(2)
    )

    location.namespace shouldBe "MyNamespace"
    location.path shouldBe s"${storageSpace.underlying}/${externalIdentifier.toString}/v2"
  }
}
