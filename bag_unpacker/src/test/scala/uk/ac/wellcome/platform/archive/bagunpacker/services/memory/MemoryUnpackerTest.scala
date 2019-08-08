package uk.ac.wellcome.platform.archive.bagunpacker.services.memory

import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.platform.archive.bagunpacker.services.{
  Unpacker,
  UnpackerTestCases
}
import uk.ac.wellcome.storage.{Identified, ObjectLocation}
import uk.ac.wellcome.storage.store.StreamStore
import uk.ac.wellcome.storage.store.memory.MemoryStreamStore
import uk.ac.wellcome.storage.streaming.{
  InputStreamWithLength,
  InputStreamWithLengthAndMetadata
}

class MemoryUnpackerTest extends UnpackerTestCases[String] {
  implicit val streamStore: MemoryStreamStore[ObjectLocation] =
    MemoryStreamStore[ObjectLocation]()
  override val unpacker: Unpacker = new MemoryUnpacker()

  override def withNamespace[R](testWith: TestWith[String, R]): R =
    testWith(randomAlphanumeric)

  // TODO: Add covariance to StreamStore
  override def withStreamStore[R](
    testWith: TestWith[StreamStore[ObjectLocation, InputStreamWithLength], R]
  ): R = {
    val store = new StreamStore[ObjectLocation, InputStreamWithLength] {
      override def get(location: ObjectLocation): ReadEither =
        streamStore
          .get(location)
          .map { is =>
            Identified(
              is.id,
              new InputStreamWithLength(
                is.identifiedT,
                length = is.identifiedT.length
              )
            )
          }

      override def put(
        location: ObjectLocation
      )(is: InputStreamWithLength): WriteEither =
        streamStore
          .put(location)(
            new InputStreamWithLengthAndMetadata(
              is,
              length = is.length,
              metadata = Map.empty
            )
          )
          .map { is =>
            is.copy(
              identifiedT = new InputStreamWithLength(
                is.identifiedT,
                length = is.identifiedT.length
              )
            )
          }
    }

    testWith(store)
  }
}
