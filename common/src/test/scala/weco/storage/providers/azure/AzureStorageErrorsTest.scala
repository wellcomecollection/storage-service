package weco.storage.providers.azure

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import weco.storage.RetryableError

class AzureStorageErrorsTest extends AnyFunSpec with Matchers {
  it("marks a timeout as a retriable error") {
    val exception = new Exception("something TimeoutException something")
    val error = AzureStorageErrors.readErrors(exception)
    error shouldBe a[RetryableError]
    error.e shouldBe exception
  }
}
