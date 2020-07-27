package uk.ac.wellcome.platform.archive.bagunpacker.services.memory

import java.io.InputStream

import uk.ac.wellcome.platform.archive.bagunpacker.services.Unpacker
import uk.ac.wellcome.storage.StorageError
import uk.ac.wellcome.storage.providers.memory.{
  MemoryLocation,
  MemoryLocationPrefix
}
import uk.ac.wellcome.storage.store.memory.MemoryStreamStore
import uk.ac.wellcome.storage.streaming.InputStreamWithLength

class MemoryUnpacker()(implicit streamStore: MemoryStreamStore[MemoryLocation])
    extends Unpacker[MemoryLocation, MemoryLocation, MemoryLocationPrefix] {
  override def get(
    location: MemoryLocation
  ): Either[StorageError, InputStream] =
    streamStore.get(location).map { _.identifiedT }

  override def put(
    location: MemoryLocation
  )(inputStream: InputStreamWithLength): Either[StorageError, Unit] =
    streamStore
      .put(location)(inputStream)
      .map { _ =>
        ()
      }
}
