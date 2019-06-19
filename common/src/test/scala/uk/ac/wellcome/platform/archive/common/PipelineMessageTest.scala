package uk.ac.wellcome.platform.archive.common

import org.scalatest.{FunSpec, Matchers, TryValues}
import PipelineMessage._
import io.circe.Json
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.json.utils.JsonAssertions

class PipelineMessageTest extends FunSpec with Matchers with TryValues with JsonAssertions {
  case class Person(name: String)

  it("can decode a PipelineMessage for a case class") {
    val jsonString =
      s"""
         |{
         |  "name": "henry",
         |  "age": 82
         |}
       """.stripMargin

    val result = fromJson[PipelineMessage[Person]](jsonString)

    val message = result.success.value
    message.payload shouldBe Person(name = "henry")
    message.json shouldBe Json.obj(
      ("name", Json.fromString("henry")),
      ("age", Json.fromInt(82))
    )
  }

  it("encodes a PipelineMessage as the underlying JSON") {
    val message = PipelineMessage(
      json = Json.obj(
        ("name", Json.fromString("silas")),
        ("age", Json.fromInt(48))
      ),
      payload = Person(name = "silas")
    )

    val jsonString = toJson(message).success.value
    assertJsonStringsAreEqual(
      jsonString,
      """
        |{
        |  "name": "silas",
        |  "age": 48
        |}
      """.stripMargin
    )
  }

  it("puts added fields in the new JSON") {
    val message = PipelineMessage(
      json = Json.obj(
        ("name", Json.fromString("silas")),
        ("age", Json.fromInt(48))
      ),
      payload = Person(name = "silas")
    )

    val updatedMessage = message.addField("birthplace", "new york")

    val jsonString = toJson(updatedMessage).success.value
    assertJsonStringsAreEqual(
      jsonString,
      """
        |{
        |  "name": "silas",
        |  "age": 48,
        |  "birthplace": "new york"
        |}
      """.stripMargin
    )
  }
}
