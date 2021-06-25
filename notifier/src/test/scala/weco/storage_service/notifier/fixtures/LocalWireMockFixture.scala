package weco.storage_service.notifier.fixtures

import com.github.tomakehurst.wiremock.client.WireMock
import weco.fixtures.TestWith

trait LocalWireMockFixture {
  val callbackHost = "localhost"
  val callbackPort = 8080

  def withLocalWireMockClient[R](testWith: TestWith[WireMock, R]): R = {
    val wireMock = new WireMock(callbackHost, callbackPort)
    wireMock.resetRequests()
    testWith(wireMock)
  }
}
