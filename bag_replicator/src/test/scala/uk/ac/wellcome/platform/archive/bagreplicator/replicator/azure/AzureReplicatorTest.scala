package uk.ac.wellcome.platform.archive.bagreplicator.replicator.azure

import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.platform.archive.bagreplicator.replicator.ReplicatorTestCases
import uk.ac.wellcome.storage.fixtures.AzureFixtures
import uk.ac.wellcome.storage.fixtures.AzureFixtures.Container
import uk.ac.wellcome.storage.store.azure.{
  NewAzureStreamStore,
  NewAzureTypedStore
}
import uk.ac.wellcome.storage.streaming.Codec._
import uk.ac.wellcome.storage.tags.Tags
import uk.ac.wellcome.storage.tags.azure.NewAzureBlobMetadata
import uk.ac.wellcome.storage.{
  AzureBlobItemLocation,
  AzureBlobItemLocationPrefix
}

class AzureReplicatorTest
    extends ReplicatorTestCases[
      Container,
      AzureBlobItemLocation,
      AzureBlobItemLocationPrefix
    ]
    with AzureFixtures {

  override def withDstNamespace[R](testWith: TestWith[Container, R]): R =
    withAzureContainer { container =>
      testWith(container)
    }

  override def withReplicator[R](testWith: TestWith[ReplicatorImpl, R]): R =
    testWith(new AzureReplicator())

  override def createDstLocationWith(
    dstContainer: Container,
    name: String
  ): AzureBlobItemLocation =
    AzureBlobItemLocation(dstContainer.name, name = name)

  override def createDstPrefixWith(
    dstContainer: Container
  ): AzureBlobItemLocationPrefix =
    AzureBlobItemLocationPrefix(dstContainer.name, namePrefix = "")

  implicit val streamStore: NewAzureStreamStore = new NewAzureStreamStore()
  override val dstStringStore: NewAzureTypedStore[String] =
    new NewAzureTypedStore[String]

  override val dstTags: Tags[AzureBlobItemLocation] = new NewAzureBlobMetadata()
}
