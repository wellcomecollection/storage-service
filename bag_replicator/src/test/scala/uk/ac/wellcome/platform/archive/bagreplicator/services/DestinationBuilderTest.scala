package uk.ac.wellcome.platform.archive.bagreplicator.services

import org.scalatest.{FunSpec, Matchers}
import uk.ac.wellcome.platform.archive.common.bagit.models.BagVersion
import uk.ac.wellcome.platform.archive.common.generators.{ExternalIdentifierGenerators, StorageSpaceGenerators}

class DestinationBuilderTest
    extends FunSpec
    with Matchers
    with ExternalIdentifierGenerators
    with StorageSpaceGenerators {

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
