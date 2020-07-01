package uk.ac.wellcome.platform.archive.bagunpacker.services.memory

import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.platform.archive.bagunpacker.services.{
  Unpacker,
  UnpackerTestCases
}
import uk.ac.wellcome.storage.ObjectLocation
import uk.ac.wellcome.storage.store.memory.MemoryStreamStore

class MemoryUnpackerTest
    extends UnpackerTestCases[MemoryStreamStore[ObjectLocation], String] {

  override def withUnpacker[R](testWith: TestWith[Unpacker, R])(
    implicit streamStore: MemoryStreamStore[ObjectLocation]
  ): R =
    testWith(
      new MemoryUnpacker()
    )

  override def withNamespace[R](testWith: TestWith[String, R]): R =
    testWith(randomAlphanumeric)

  override def withStreamStore[R](
    testWith: TestWith[MemoryStreamStore[ObjectLocation], R]
  ): R =
    testWith(MemoryStreamStore[ObjectLocation]())
}
