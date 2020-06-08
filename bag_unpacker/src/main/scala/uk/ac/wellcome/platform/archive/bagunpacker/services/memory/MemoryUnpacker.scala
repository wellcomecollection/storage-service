package uk.ac.wellcome.platform.archive.bagunpacker.services.memory

import java.io.InputStream

import uk.ac.wellcome.platform.archive.bagunpacker.services.Unpacker
import uk.ac.wellcome.storage.{ObjectLocation, StorageError}
import uk.ac.wellcome.storage.store.memory.MemoryStreamStore
import uk.ac.wellcome.storage.streaming.InputStreamWithLength

class MemoryUnpacker()(implicit streamStore: MemoryStreamStore[ObjectLocation])
    extends Unpacker {
  override def get(
    location: ObjectLocation
  ): Either[StorageError, InputStream] =
    streamStore.get(location).map { _.identifiedT }

  override def put(
    location: ObjectLocation
  )(inputStream: InputStreamWithLength): Either[StorageError, Unit] =
    streamStore
      .put(location)(inputStream)
      .map { _ =>
        ()
      }

  override def formatLocation(location: ObjectLocation): String =
    s"mem://$location"
}
