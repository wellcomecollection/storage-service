package weco.storage.store.azure

import java.io.InputStream

import weco.fixtures.TestWith
import weco.storage.fixtures.AzureFixtures
import weco.storage.fixtures.AzureFixtures.Container
import weco.storage.generators.{Record, RecordGenerators}
import weco.storage.providers.azure.AzureBlobLocation
import weco.storage.store.TypedStoreTestCases
import weco.storage.streaming.{Codec, InputStreamWithLength}
import weco.storage.{Identified, StoreReadError, StoreWriteError}

class AzureTypedStoreTest
    extends TypedStoreTestCases[
      AzureBlobLocation,
      Record,
      Container,
      AzureStreamStore,
      AzureTypedStore[Record],
      Unit]
    with RecordGenerators
    with AzureFixtures {
  override def withBrokenStreamStore[R](
    testWith: TestWith[AzureStreamStore, R]): R = {
    val brokenStore = new AzureStreamStore() {
      override def get(location: AzureBlobLocation): ReadEither =
        Left(StoreReadError(new Throwable("get: BOOM!")))

      override def put(location: AzureBlobLocation)(
        is: InputStreamWithLength): WriteEither =
        Left(StoreWriteError(new Throwable("put: BOOM!")))
    }

    testWith(brokenStore)
  }

  override def createT: Record = createRecord

  override def withSingleValueStreamStore[R](rawStream: InputStream)(
    testWith: TestWith[AzureStreamStore, R]): R = {
    val store = new AzureStreamStore() {
      override def get(location: AzureBlobLocation): ReadEither =
        Right(
          Identified(
            location,
            new InputStreamWithLength(rawStream, length = 0)))
    }

    testWith(store)
  }

  override def withTypedStore[R](
    streamStore: AzureStreamStore,
    initialEntries: Map[AzureBlobLocation, Record])(
    testWith: TestWith[AzureTypedStore[Record], R])(
    implicit codec: Codec[Record]): R = {
    implicit val s: AzureStreamStore = streamStore
    val typedStore = new AzureTypedStore[Record]()

    initialEntries.foreach {
      case (location, record) =>
        typedStore.put(location)(record) shouldBe a[Right[_, _]]
    }

    testWith(typedStore)
  }

  override def withStreamStoreImpl[R](
    context: Unit,
    initialEntries: Map[AzureBlobLocation, InputStreamWithLength])(
    testWith: TestWith[AzureStreamStore, R]): R = {
    val streamStore = new AzureStreamStore()

    initialEntries.foreach {
      case (location, inputStream) =>
        streamStore.put(location)(inputStream) shouldBe a[Right[_, _]]
    }

    testWith(streamStore)
  }

  override def withStreamStoreContext[R](testWith: TestWith[Unit, R]): R =
    testWith(())

  override def withNamespace[R](testWith: TestWith[Container, R]): R =
    withAzureContainer { container =>
      testWith(container)
    }

  override def createId(implicit container: Container): AzureBlobLocation =
    createAzureBlobLocationWith(container)
}
