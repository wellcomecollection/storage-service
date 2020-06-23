package uk.ac.wellcome.storage.store.memory

import uk.ac.wellcome.storage.{Identified, MemoryLocation, ObjectLocation}
import uk.ac.wellcome.storage.store.{StreamStore, TypedStore}
import uk.ac.wellcome.storage.streaming.{Codec, InputStreamWithLength}

// TODO: Bridging code while we split ObjectLocation.  Remove this later.
// See https://github.com/wellcomecollection/platform/issues/4596
class NewMemoryTypedStore[T](
  implicit
  underlying: MemoryStreamStore[ObjectLocation],
  val codec: Codec[T]
) extends TypedStore[MemoryLocation, T] {
  override implicit val streamStore: StreamStore[MemoryLocation] =
    new StreamStore[MemoryLocation] {
      override def get(location: MemoryLocation): ReadEither =
    underlying
      .get(location.toObjectLocation)
      .map { case Identified(_, result) => Identified(location, result) }

      override def put(location: MemoryLocation)(inputStream: InputStreamWithLength): WriteEither =
        underlying
          .put(location.toObjectLocation)(inputStream)
          .map { case Identified(_, result) => Identified(location, result) }
    }
}
