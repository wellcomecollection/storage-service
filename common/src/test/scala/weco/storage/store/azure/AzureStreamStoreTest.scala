package weco.storage.store.azure

import weco.fixtures.TestWith
import weco.storage.OverwriteError
import weco.storage.fixtures.AzureFixtures
import weco.storage.fixtures.AzureFixtures.Container
import weco.storage.providers.azure.AzureBlobLocation
import weco.storage.store.StreamStoreTestCases
import weco.storage.streaming.InputStreamWithLength

case class AzureStreamStoreContext(
  allowOverwrite: Option[Boolean] = None
)

class AzureStreamStoreTest
    extends StreamStoreTestCases[
      AzureBlobLocation,
      Container,
      AzureStreamStore,
      AzureStreamStoreContext]
    with AzureFixtures {

  // Azurite test container does not error when handed incorrect stream lengths
  override lazy val skipStreamLengthTests = true

  override def withNamespace[R](testWith: TestWith[Container, R]): R =
    withAzureContainer { container =>
      testWith(container)
    }

  override def createId(implicit container: Container): AzureBlobLocation =
    createAzureBlobLocationWith(container)

  override def withStreamStoreImpl[R](
    context: AzureStreamStoreContext,
    initialEntries: Map[AzureBlobLocation, InputStreamWithLength])(
    testWith: TestWith[AzureStreamStore, R]): R = {
    initialEntries.foreach {
      case (location, data) =>
        azureClient
          .getBlobContainerClient(location.container)
          .getBlobClient(location.name)
          .upload(data, data.length)
    }

    val store = context.allowOverwrite match {
      case Some(allowOverwrites) =>
        new AzureStreamStore(allowOverwrites = allowOverwrites)
      case None => new AzureStreamStore()
    }

    testWith(store)
  }

  override def withStreamStoreContext[R](
    testWith: TestWith[AzureStreamStoreContext, R]): R =
    testWith(AzureStreamStoreContext())

  describe("allowOverwrites is false") {
    it("will not overwrite an existing object") {
      withNamespace { implicit namespace =>
        val id = createId
        val initialEntry = ReplayableStream(randomBytes(256))
        val updatedEntry = ReplayableStream(randomBytes(256))

        withStoreImpl(
          initialEntries = Map(id -> initialEntry),
          storeContext = AzureStreamStoreContext(
            allowOverwrite = Some(false)
          )
        ) { store =>
          val putResult = store.put(id)(updatedEntry).left.value

          putResult shouldBe a[OverwriteError]

          val retrievedEntry = store.get(id).right.get

          assertEqualT(initialEntry, retrievedEntry.identifiedT)
        }
      }
    }
  }
}
