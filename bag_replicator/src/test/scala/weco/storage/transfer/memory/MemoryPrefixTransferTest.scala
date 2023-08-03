package weco.storage.transfer.memory

import weco.fixtures.TestWith
import weco.storage.ListingFailure
import weco.storage.generators.{
  MemoryLocationGenerators,
  Record,
  RecordGenerators
}
import weco.storage.providers.memory.{MemoryLocation, MemoryLocationPrefix}
import weco.storage.store.memory.MemoryStore
import weco.storage.transfer._
import weco.storage.transfer.PrefixTransferTestCases

class MemoryPrefixTransferTest
    extends PrefixTransferTestCases[
      MemoryLocation,
      MemoryLocationPrefix,
      MemoryLocation,
      MemoryLocationPrefix,
      Record,
      String,
      String,
      MemoryStore[MemoryLocation, Record] with MemoryPrefixTransfer[
        MemoryLocation,
        MemoryLocationPrefix,
        Record],
      MemoryStore[MemoryLocation, Record] with MemoryPrefixTransfer[
        MemoryLocation,
        MemoryLocationPrefix,
        Record],
      MemoryStore[MemoryLocation, Record] with MemoryPrefixTransfer[
        MemoryLocation,
        MemoryLocationPrefix,
        Record]]
    with RecordGenerators
    with MemoryLocationGenerators {

  type MemoryRecordStore =
    MemoryStore[MemoryLocation, Record] with MemoryPrefixTransfer[
      MemoryLocation,
      MemoryLocationPrefix,
      Record]

  override def withSrcNamespace[R](testWith: TestWith[String, R]): R =
    testWith(randomAlphanumeric())

  override def withDstNamespace[R](testWith: TestWith[String, R]): R =
    testWith(randomAlphanumeric())

  override def createSrcLocation(srcNamespace: String): MemoryLocation =
    createMemoryLocationWith(srcNamespace)

  override def createDstLocation(dstNamespace: String): MemoryLocation =
    createMemoryLocationWith(dstNamespace)

  override def createSrcPrefix(srcNamespace: String): MemoryLocationPrefix =
    createMemoryLocationPrefixWith(srcNamespace)

  override def createDstPrefix(dstNamespace: String): MemoryLocationPrefix =
    createMemoryLocationPrefixWith(dstNamespace)

  override def createSrcLocationFrom(srcPrefix: MemoryLocationPrefix,
                                     suffix: String): MemoryLocation =
    srcPrefix.asLocation(suffix)

  override def createDstLocationFrom(dstPrefix: MemoryLocationPrefix,
                                     suffix: String): MemoryLocation =
    dstPrefix.asLocation(suffix)

  override def withSrcStore[R](initialEntries: Map[MemoryLocation, Record])(
    testWith: TestWith[MemoryRecordStore, R])(
    implicit underlying: MemoryRecordStore): R = {
    initialEntries.foreach {
      case (location, record) =>
        underlying.put(location)(record) shouldBe a[Right[_, _]]
    }

    testWith(underlying)
  }

  override def withDstStore[R](initialEntries: Map[MemoryLocation, Record])(
    testWith: TestWith[MemoryRecordStore, R])(
    implicit underlying: MemoryRecordStore): R = {
    initialEntries.foreach {
      case (location, record) =>
        underlying.put(location)(record) shouldBe a[Right[_, _]]
    }

    testWith(underlying)
  }

  class MemoryMemoryLocationPrefixTransfer(
    initialEntries: Map[MemoryLocation, Record])
      extends MemoryStore[MemoryLocation, Record](
        initialEntries = initialEntries)
      with MemoryPrefixTransfer[MemoryLocation, MemoryLocationPrefix, Record] {
    override protected def startsWith(location: MemoryLocation,
                                      prefix: MemoryLocationPrefix): Boolean = {
      location.namespace == prefix.namespace && location.path.startsWith(
        prefix.path)
    }

    override protected def buildDstLocation(
      srcPrefix: MemoryLocationPrefix,
      dstPrefix: MemoryLocationPrefix,
      srcLocation: MemoryLocation
    ): MemoryLocation =
      dstPrefix.asLocation(
        srcLocation.path.stripPrefix(srcPrefix.path)
      )
  }

  override def withPrefixTransfer[R](srcStore: MemoryRecordStore,
                                     dstStore: MemoryRecordStore)(
    testWith: TestWith[PrefixTransfer[MemoryLocationPrefix,
                                      MemoryLocation,
                                      MemoryLocationPrefix,
                                      MemoryLocation],
                       R]): R =
    testWith(srcStore)

  override def withExtraListingTransfer[R](
    srcStore: MemoryRecordStore,
    dstStore: MemoryRecordStore
  )(
    testWith: TestWith[PrefixTransferImpl, R]
  ): R = {
    val prefixTransfer = new MemoryMemoryLocationPrefixTransfer(
      initialEntries = srcStore.entries ++ dstStore.entries) {
      override def list(prefix: MemoryLocationPrefix): ListingResult = {
        val matchingLocations = entries
          .filter { case (location, _) => startsWith(location, prefix) }
          .map { case (location, _) => location }

        Right(matchingLocations ++ Seq(createMemoryLocation))
      }
    }

    testWith(prefixTransfer)
  }

  override def withBrokenListingTransfer[R](
    srcStore: MemoryRecordStore,
    dstStore: MemoryRecordStore
  )(
    testWith: TestWith[PrefixTransferImpl, R]
  ): R = {
    val prefixTransfer = new MemoryMemoryLocationPrefixTransfer(
      initialEntries = srcStore.entries ++ dstStore.entries) {
      override def list(prefix: MemoryLocationPrefix): ListingResult =
        Left(ListingFailure(prefix, e = new Throwable("BOOM!")))
    }

    testWith(prefixTransfer)
  }

  override def withBrokenTransfer[R](
    srcStore: MemoryRecordStore,
    dstStore: MemoryRecordStore
  )(
    testWith: TestWith[PrefixTransferImpl, R]
  ): R = {
    val prefixTransfer = new MemoryMemoryLocationPrefixTransfer(
      initialEntries = srcStore.entries ++ dstStore.entries) {
      override def transfer(src: MemoryLocation, dst: MemoryLocation): TransferEither =
        Left(TransferSourceFailure(src, dst))
    }

    testWith(prefixTransfer)
  }

  override def withContext[R](testWith: TestWith[MemoryRecordStore, R]): R =
    testWith(
      new MemoryMemoryLocationPrefixTransfer(initialEntries = Map.empty)
    )

  override def createT: Record = createRecord
}
