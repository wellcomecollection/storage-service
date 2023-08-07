package weco.storage.generators

import weco.fixtures.RandomGenerators
import weco.storage.fixtures.AzureFixtures.Container
import weco.storage.providers.azure.{AzureBlobLocation, AzureBlobLocationPrefix}

trait AzureBlobLocationGenerators extends RandomGenerators {

  /** Create a valid container name, which means:
    *
    *   - all lowercase
    *   - between 3 and 63 characters long
    *   - only contains letters, numbers, and the dash (-) character
    *
    * See https://docs.microsoft.com/en-us/rest/api/storageservices/Naming-and-Referencing-Containers--Blobs--and-Metadata
    *
    */
  def createContainerName: String =
    randomAlphanumeric(length = randomInt(from = 3, to = 63)).toLowerCase

  def createContainer: Container =
    Container(createContainerName)

  def createAzureBlobLocationWith(container: Container): AzureBlobLocation =
    AzureBlobLocation(
      container = container.name,
      name = randomAlphanumeric()
    )

  def createAzureBlobLocationPrefixWith(
    container: Container): AzureBlobLocationPrefix =
    AzureBlobLocationPrefix(
      container = container.name,
      namePrefix = randomAlphanumeric()
    )

  def createAzureBlobLocationPrefix: AzureBlobLocationPrefix =
    createAzureBlobLocationPrefixWith(createContainer)
}
