package weco.storage_service.bag_unpacker.services.memory

import weco.fixtures.TestWith
import weco.storage_service.bag_unpacker.services.{Unpacker, UnpackerTestCases}
import weco.storage.providers.memory.{MemoryLocation, MemoryLocationPrefix}
import weco.storage.store.memory.MemoryStreamStore

class MemoryUnpackerTest
    extends UnpackerTestCases[
      MemoryLocation,
      MemoryLocationPrefix,
      MemoryStreamStore[MemoryLocation],
      String
    ] {

  override def withUnpacker[R](
    testWith: TestWith[
      Unpacker[MemoryLocation, MemoryLocation, MemoryLocationPrefix],
      R
    ]
  )(
    implicit streamStore: MemoryStreamStore[MemoryLocation]
  ): R =
    testWith(
      new MemoryUnpacker()
    )

  override def withNamespace[R](testWith: TestWith[String, R]): R =
    testWith(randomAlphanumeric())

  override def withStreamStore[R](
    testWith: TestWith[MemoryStreamStore[MemoryLocation], R]
  ): R =
    testWith(MemoryStreamStore[MemoryLocation]())

  override def createSrcLocationWith(
    namespace: String,
    path: String
  ): MemoryLocation =
    MemoryLocation(namespace = namespace, path = path)

  override def createDstPrefixWith(
    namespace: String,
    pathPrefix: String
  ): MemoryLocationPrefix =
    MemoryLocationPrefix(namespace = namespace, path = pathPrefix)

  override def listKeysUnder(
    prefix: MemoryLocationPrefix
  )(implicit store: MemoryStreamStore[MemoryLocation]): Seq[String] =
    store.memoryStore.entries.keys
      .filter { loc =>
        loc.namespace == prefix.namespace && loc.path.startsWith(
          prefix.pathPrefix
        )
      }
      .map { _.path }
      .toSeq
}
