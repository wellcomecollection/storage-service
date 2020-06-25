package uk.ac.wellcome.platform.archive.common.storage.services.memory

import uk.ac.wellcome.platform.archive.common.bagit.models.{BagId, BagVersion}
import uk.ac.wellcome.platform.archive.common.storage.models.StorageManifest
import uk.ac.wellcome.platform.archive.common.storage.services.StorageManifestDao

import uk.ac.wellcome.storage.{ReadError, Version}
import uk.ac.wellcome.storage.store.memory.{MemoryStore, MemoryVersionedStore}

class MemoryStorageManifestDao(
  val vhs: MemoryVersionedStore[BagId, StorageManifest]
) extends StorageManifestDao {
  override def listVersions(
    bagId: BagId,
    before: Option[BagVersion]
  ): Either[ReadError, Seq[StorageManifest]] = {
    val underlying =
      vhs.store
        .asInstanceOf[MemoryStore[
          Version[BagId, Int],
          StorageManifest
        ]]

    Right(
      underlying.entries
        .filter {
          case (_, manifest) => manifest.id == bagId
        }
        .map { case (_, manifest) => manifest }
        .toSeq
        .filter { manifest =>
          before match {
            case Some(beforeVersion) =>
              manifest.version.underlying < beforeVersion.underlying
            case _ => true
          }
        }
        .sortBy { _.version.underlying }
        .reverse
    )
  }
}
