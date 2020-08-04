package uk.ac.wellcome.platform.archive.bagverifier.services.azure

import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.platform.archive.bagverifier.services.{ReplicatedBagVerifier, ReplicatedBagVerifierTestCases}
import uk.ac.wellcome.platform.archive.common.bagit.services.BagReader
import uk.ac.wellcome.platform.archive.common.bagit.services.azure.AzureBagReader
import uk.ac.wellcome.platform.archive.common.fixtures.BagBuilder
import uk.ac.wellcome.platform.archive.common.fixtures.azure.AzureBagBuilder
import uk.ac.wellcome.platform.archive.common.fixtures.s3.S3BagBuilder
import uk.ac.wellcome.storage.azure.{AzureBlobLocation, AzureBlobLocationPrefix}
import uk.ac.wellcome.storage.fixtures.AzureFixtures.Container
import uk.ac.wellcome.storage.fixtures.S3Fixtures.Bucket
import uk.ac.wellcome.storage.fixtures.{AzureFixtures, S3Fixtures}
import uk.ac.wellcome.storage.s3.{S3ObjectLocation, S3ObjectLocationPrefix}
import uk.ac.wellcome.storage.store.TypedStore
import uk.ac.wellcome.storage.store.azure.{AzureStreamStore, AzureTypedStore}
import uk.ac.wellcome.storage.store.s3.S3TypedStore
import uk.ac.wellcome.storage.streaming.Codec
import uk.ac.wellcome.storage.streaming.Codec._

class AzureReplicatedBagVerifierTests extends ReplicatedBagVerifierTestCases[
  S3ObjectLocation,
  S3ObjectLocationPrefix,
  Bucket,
  AzureBlobLocation,
  AzureBlobLocationPrefix,
  Container
] with S3Fixtures with AzureFixtures{

  val s3TypedStore = S3TypedStore[String]
  val azureTypedStore = new AzureTypedStore[String]()(implicitly[Codec[String]], new AzureStreamStore())


  override protected def copyTagManifest(srcRoot: S3ObjectLocationPrefix, replicaRoot: AzureBlobLocationPrefix): Unit = {
    for {
      identified <- s3TypedStore.get(srcRoot.asLocation("tagmanifest-sha256.txt"))
      _ <-azureTypedStore.put(AzureBlobLocation(replicaRoot.container, s"${replicaRoot.namePrefix}/tagmanifest-sha256.txt"))(identified.identifiedT)
    }yield()
  }

  override def createSrcPrefix(implicit bucket: Bucket): S3ObjectLocationPrefix = createS3ObjectLocationPrefixWith(bucket)

  override def withSrcNamespace[R](testWith: TestWith[Bucket, R]): R = withLocalS3Bucket { bucket =>
    testWith(bucket)
  }

  override def withReplicaNamespace[R](testWith: TestWith[Container, R]): R = withAzureContainer { container =>
    testWith(container)
  }

  override def withSrcTypedStore[R](testWith: TestWith[TypedStore[S3ObjectLocation, String], R]): R = testWith(S3TypedStore[String])

  override def withReplicaTypedStore[R](testWith: TestWith[TypedStore[AzureBlobLocation, String], R]): R = testWith(azureTypedStore)

  override val srcBagBuilder: BagBuilder[S3ObjectLocation, S3ObjectLocationPrefix, Bucket] = new S3BagBuilder {}
  override val replicaBagBuilder: BagBuilder[AzureBlobLocation, AzureBlobLocationPrefix, Container] = new AzureBagBuilder {}

  override def withVerifier[R](container: Container)(testWith: TestWith[ReplicatedBagVerifier[S3ObjectLocation, S3ObjectLocationPrefix, AzureBlobLocation, AzureBlobLocationPrefix], R])(implicit typedStore: TypedStore[AzureBlobLocation, String]): R =
    testWith(
      new AzureReplicatedBagVerifier(namespace = container.name)
    )

  override def writeFile(location: AzureBlobLocation, contents: String): Unit = azureTypedStore.put(location)(contents)

  override def createBagReader: BagReader[AzureBlobLocation, AzureBlobLocationPrefix] = new AzureBagReader()

  override def createId(implicit container: Container): AzureBlobLocation = createAzureBlobLocationWith(container)
}
