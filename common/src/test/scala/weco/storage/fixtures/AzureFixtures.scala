package weco.storage.fixtures

import com.azure.storage.blob.{BlobServiceClient, BlobServiceClientBuilder}
import org.scalatest.concurrent.{Eventually, IntegrationPatience}
import weco.fixtures.{fixture, Fixture}
import weco.storage.generators.AzureBlobLocationGenerators

object AzureFixtures {
  class Container(val name: String) extends AnyVal {
    override def toString: String = s"AzureFixtures.Container($name)"
  }

  object Container {
    def apply(name: String): Container = new Container(name)
  }
}

trait AzureFixtures
    extends Eventually
    with IntegrationPatience
    with AzureBlobLocationGenerators {
  import AzureFixtures._

  implicit val azureClient: BlobServiceClient =
    new BlobServiceClientBuilder()
      .connectionString("UseDevelopmentStorage=true;")
      .buildClient()

  def withAzureContainer[R]: Fixture[Container, R] =
    fixture[Container, R](
      create = {
        val containerName: String = createContainerName
        azureClient.createBlobContainer(containerName)

        Container(containerName)
      },
      destroy = { container: Container =>
        azureClient.deleteBlobContainer(container.name)
      }
    )
}
