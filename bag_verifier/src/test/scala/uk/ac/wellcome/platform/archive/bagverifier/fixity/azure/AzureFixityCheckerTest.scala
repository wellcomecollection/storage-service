package uk.ac.wellcome.platform.archive.bagverifier.fixity.azure

import java.net.URI

import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.platform.archive.bagverifier.fixity.{
  FixityChecker,
  FixityCheckerTagsTestCases
}
import uk.ac.wellcome.platform.archive.bagverifier.storage.azure.AzureResolvable
import uk.ac.wellcome.platform.archive.common.verify._
import uk.ac.wellcome.storage.azure.{AzureBlobLocation, AzureBlobLocationPrefix}
import uk.ac.wellcome.storage.fixtures.AzureFixtures
import uk.ac.wellcome.storage.fixtures.AzureFixtures.Container
import uk.ac.wellcome.storage.store.azure.{AzureStreamStore, AzureTypedStore}

class AzureFixityCheckerTest
    extends FixityCheckerTagsTestCases[
      AzureBlobLocation,
      AzureBlobLocationPrefix,
      Container,
      Unit,
      AzureStreamStore
    ]
    with AzureFixtures {

  val azureTypedStore: AzureTypedStore[String] = AzureTypedStore[String]

  override def withContext[R](testWith: TestWith[Unit, R]): R = testWith(())

  override def putString(location: AzureBlobLocation, contents: String)(
    implicit context: Unit
  ): Unit = azureTypedStore.put(location)(contents)

  override def withFixityChecker[R](azureReader: AzureStreamStore)(
    testWith: TestWith[
      FixityChecker[AzureBlobLocation, AzureBlobLocationPrefix],
      R
    ]
  )(implicit context: Unit): R =
    testWith(new AzureFixityChecker() {
      // We need to override the underlying StreamStore so Mockito can spy
      // on its interactions during the tests.
      override val streamReader: AzureStreamStore =
        azureReader
    })

  override def withStreamReader[R](
    testWith: TestWith[AzureStreamStore, R]
  )(implicit context: Unit): R =
    testWith(new AzureStreamStore())

  override def resolve(location: AzureBlobLocation): URI =
    new AzureResolvable().resolve(location)

  override def withNamespace[R](testWith: TestWith[Container, R]): R =
    withAzureContainer { container =>
      testWith(container)
    }

  override def createId(implicit container: Container): AzureBlobLocation =
    createAzureBlobLocationWith(container)

  override def tagName(algorithm: HashingAlgorithm): String =
    algorithm match {
      case MD5    => "ContentMD5"
      case SHA1   => "ContentSHA1"
      case SHA256 => "ContentSHA256"
      case SHA512 => "ContentSHA512"
    }
}
