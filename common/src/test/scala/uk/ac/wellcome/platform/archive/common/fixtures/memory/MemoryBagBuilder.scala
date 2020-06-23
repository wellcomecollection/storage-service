package uk.ac.wellcome.platform.archive.common.fixtures.memory

import uk.ac.wellcome.platform.archive.common.bagit.models.{BagVersion, ExternalIdentifier}
import uk.ac.wellcome.platform.archive.common.fixtures.BetterBagBuilder
import uk.ac.wellcome.platform.archive.common.storage.models.StorageSpace
import uk.ac.wellcome.platform.archive.common.storage.services.DestinationBuilder
import uk.ac.wellcome.storage.{MemoryLocation, MemoryLocationPrefix}

import scala.util.Random

trait MemoryBagBuilder
  extends BetterBagBuilder[MemoryLocation, MemoryLocationPrefix] {

  override def createBagRoot(
    space: StorageSpace,
    externalIdentifier: ExternalIdentifier,
    version: BagVersion
  )(
    implicit namespace: String
  ): MemoryLocationPrefix =
    MemoryLocationPrefix(
      namespace = namespace,
      pathPrefix = DestinationBuilder.buildPath(space, externalIdentifier, version)
    )

  override protected def buildFetchEntryLine(
    entry: PayloadEntry
  )(implicit namespace: String): String = {
    val displaySize =
      if (Random.nextBoolean()) entry.contents.getBytes.length.toString else "-"

    s"""mem://$namespace/${entry.path} $displaySize ${entry.bagPath}"""
  }
}
