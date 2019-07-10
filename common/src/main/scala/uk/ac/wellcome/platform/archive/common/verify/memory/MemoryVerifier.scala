package uk.ac.wellcome.platform.archive.common.verify.memory

import java.net.URI

import uk.ac.wellcome.platform.archive.common.storage.LocateFailure
import uk.ac.wellcome.platform.archive.common.verify.Verifier
import uk.ac.wellcome.storage.ObjectLocation
import uk.ac.wellcome.storage.store.memory.MemoryStreamStore
import uk.ac.wellcome.storage.streaming.InputStreamWithLengthAndMetadata

class MemoryVerifier(val streamStore: MemoryStreamStore[ObjectLocation])
    extends Verifier[InputStreamWithLengthAndMetadata] {
  override def locate(uri: URI): Either[LocateFailure[URI], ObjectLocation] =
    Right(
      ObjectLocation(
        namespace = uri.getHost,
        path = uri.getPath.stripPrefix("/")
      )
    )
}
