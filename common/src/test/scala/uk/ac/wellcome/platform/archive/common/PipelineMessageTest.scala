package uk.ac.wellcome.platform.archive.common

import io.circe.Json
import io.circe.parser.parse
import org.scalatest.{EitherValues, FunSpec, Matchers}
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.json.utils.JsonAssertions

class PipelineMessageTest extends FunSpec with Matchers with EitherValues with JsonAssertions {
  case class Person(name: String)

  it("can decode a PipelineMessage for a case class") {
    val jsonString =
      s"""
         |{
         |  "name": "henry",
         |  "age": 82
         |}
       """.stripMargin

    val result = PipelineMessage.fromJson[Person](parse(jsonString).right.value)

    val message = result.right.value
    message.payload shouldBe Person(name = "henry")
    message.json shouldBe Json.obj(
      ("name", Json.fromString("henry")),
      ("age", Json.fromInt(82))
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

    val jsonString = updatedMessage.json.noSpaces
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
