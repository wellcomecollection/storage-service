package uk.ac.wellcome.platform.archive.bag_indexer.elasticsearch

import com.sksamuel.elastic4s.ElasticDsl._
import com.sksamuel.elastic4s.requests.mappings.{KeywordField, MappingDefinition, ObjectField}

trait IndexConfig {
  val mapping: MappingDefinition
}

case object ManifestIndexConfig extends IndexConfig {
  def keywordFieldWithText(name: String): KeywordField =
    keywordField(name).fields(textField("text"))

  private val infoFields: ObjectField =
    objectField("info").fields(
      dateField("baggingDate"),
      textField("externalDescription"),
      keywordFieldWithText("externalIdentifier"),
      textField("internalSenderDescription"),
      keywordFieldWithText("internalSenderIdentifier"),

      // It seems unlikely that anybody would want to search on the Payload-Oxum field,
      // but we include it for completeness.
      keywordField("payloadOxum"),

      keywordField("type")
    )

  private def locationFields(name: String): ObjectField =
    objectField(name).fields(
      keywordField("bucket"),
      keywordField("path"),
      objectField("provider").fields(
        keywordField("id"),
        keywordField("type")
      ),
      keywordField("type")
    )

  private def manifestFields(name: String): ObjectField =
    objectField(name).fields(
      keywordField("checksumAlgorithm"),
      nestedField("files").fields(
        keywordField("checksum"),
        keywordFieldWithText("name"),
        keywordField("path"),
        intField("size"),
        keywordField("type")
      ),
      keywordField("type")
    )

  private val spaceFields: ObjectField =
    objectField("space").fields(
      keywordField("id"),
      keywordField("type")
    )

  val mapping: MappingDefinition = properties(
    keywordField("@context"),
    dateField("createdDate"),
    keywordField("id"),
    infoFields,
    locationFields("location"),
    manifestFields("manifest"),
    locationFields("replicaLocations"),
    spaceFields,
    manifestFields("tagManifest"),
    keywordField("type"),
    keywordField("version")
  )
}
