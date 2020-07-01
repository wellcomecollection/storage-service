package uk.ac.wellcome.platform.archive.bagunpacker.services.memory

import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.platform.archive.bagunpacker.services.{
  Unpacker,
  UnpackerTestCases
}
import uk.ac.wellcome.storage.ObjectLocation
import uk.ac.wellcome.storage.store.StreamStore
import uk.ac.wellcome.storage.store.memory.MemoryStreamStore

class MemoryUnpackerTest extends UnpackerTestCases[String] {
  implicit val streamStore: MemoryStreamStore[ObjectLocation] =
    MemoryStreamStore[ObjectLocation]()
  override val unpacker: Unpacker = new MemoryUnpacker()

  override def withNamespace[R](testWith: TestWith[String, R]): R =
    testWith(randomAlphanumeric)

  // TODO: Should we be sharing an instance between tests?
  override def withStreamStore[R](testWith: TestWith[StreamStore[ObjectLocation], R]): R =
    testWith(streamStore)
}
