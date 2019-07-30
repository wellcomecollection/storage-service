import uk.ac.wellcome.platform.archive.common.bagit.models.{BagVersion, ExternalIdentifier}
import uk.ac.wellcome.platform.archive.common.generators.{ExternalIdentifierGenerators, StorageSpaceGenerators}
import uk.ac.wellcome.platform.archive.common.storage.models.StorageSpace

import scala.util.Random

case class BagEntry(path: String, contents: String)

object BagBuilder extends StorageSpaceGenerators with ExternalIdentifierGenerators {
  def createBagWith(
    space: StorageSpace = createStorageSpace,
    externalIdentifier: ExternalIdentifier = createExternalIdentifier,
    version: BagVersion = BagVersion(randomInt(from = 2, to = 10)),
    payloadFileCount: Int = randomInt(from = 5, to = 50)
  ): Seq[BagEntry] = {

    val fetchEntryCount = randomInt(from = 0, to = payloadFileCount)

    val payloadFiles = createPayloadFiles(
      space = space,
      externalIdentifier = externalIdentifier,
      version = version,
      payloadFileCount = payloadFileCount - fetchEntryCount
    )

    val fetchEntries = createPayloadFiles(
      space = space,
      externalIdentifier = externalIdentifier,
      version = version.copy(underlying = version.underlying - 1),
      payloadFileCount = fetchEntryCount
    )

    payloadFiles ++ fetchEntries
  }

  private def createPayloadFiles(
    space: StorageSpace = createStorageSpace,
    externalIdentifier: ExternalIdentifier = createExternalIdentifier,
    version: BagVersion = BagVersion(randomInt(from = 2, to = 10)),
    payloadFileCount: Int): Seq[BagEntry] = {

    // The structure of bags in the replicator bucket is currently
    // of the form
    //
    //    {space}/{externalIdentifier}/{version}
    //
    val bagRoot = s"$space/$externalIdentifier/$version"

    (1 to payloadFileCount).map { _ =>
      BagEntry(
        path = s"$bagRoot/data/$randomPath",
        contents = Random.nextString(length = randomInt(1, 256))
      )
    }
  }

  private def randomPath: String =
    (1 to randomInt(from = 1, to = 5))
      .map { _ => randomAlphanumeric }
      .mkString("/")
}

println(BagBuilder.createBagWith())