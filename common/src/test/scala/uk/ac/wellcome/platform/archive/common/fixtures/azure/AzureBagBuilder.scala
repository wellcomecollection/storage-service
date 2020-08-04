package uk.ac.wellcome.platform.archive.common.fixtures.azure

import uk.ac.wellcome.platform.archive.common.bagit.models.{
  BagVersion,
  ExternalIdentifier
}
import uk.ac.wellcome.platform.archive.common.fixtures.{
  BagBuilder,
  PayloadEntry
}
import uk.ac.wellcome.platform.archive.common.storage.models.StorageSpace
import uk.ac.wellcome.storage.azure.{AzureBlobLocation, AzureBlobLocationPrefix}
import uk.ac.wellcome.storage.fixtures.AzureFixtures
import uk.ac.wellcome.storage.fixtures.AzureFixtures.Container

import scala.util.Random

trait AzureBagBuilder
    extends BagBuilder[AzureBlobLocation, AzureBlobLocationPrefix, Container]
    with AzureFixtures {
  override def createBagRoot(
    space: StorageSpace,
    externalIdentifier: ExternalIdentifier,
    version: BagVersion
  )(implicit container: Container): AzureBlobLocationPrefix =
    AzureBlobLocationPrefix(
      container = container.name,
      namePrefix = createBagRootPath(space, externalIdentifier, version)
    )

  override def createBagLocation(
    bagRoot: AzureBlobLocationPrefix,
    path: String
  ): AzureBlobLocation = AzureBlobLocation(bagRoot.container, path)

  override def buildFetchEntryLine(
    entry: PayloadEntry
  )(implicit container: Container): String = {
    val displaySize =
      if (Random.nextBoolean()) entry.contents.getBytes.length.toString else "-"

    s"""https://myaccount.blob.core.windows.net/${container.name}/${entry.path} $displaySize ${entry.bagPath}"""
  }
}
