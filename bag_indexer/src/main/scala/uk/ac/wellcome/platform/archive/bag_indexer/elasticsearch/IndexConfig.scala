package uk.ac.wellcome.platform.archive.bag_indexer.elasticsearch

import com.sksamuel.elastic4s.ElasticDsl._
import com.sksamuel.elastic4s.requests.mappings.{
  KeywordField,
  MappingDefinition,
  ObjectField
}

trait IndexConfig {
  protected def keywordFieldWithText(name: String): KeywordField =
    keywordField(name).fields(textField("text"))

  val mapping: MappingDefinition
}

case object ManifestIndexConfig extends IndexConfig {
  private def bagInfoField(name: String): ObjectField =
    objectField(name).fields(textField("underlying"))

  private val infoFields: ObjectField =
    objectField("info").fields(
      keywordFieldWithText("externalIdentifier"),
      objectField("payloadOxum").fields(
        intField("numberOfPayloadFiles"),
        longField("payloadBytes")
      ),
      dateField("baggingDate"),
      bagInfoField("sourceOrganisation"),
      bagInfoField("externalDescription"),
      bagInfoField("internalSenderIdentifier"),
      bagInfoField("internalSenderDescription")
    )

  private def locationFields(name: String): ObjectField =
    objectField(name).fields(
      objectField("provider").fields(
        keywordField("id"),
        keywordField("type")
      ),
      objectField("prefix").fields(
        keywordField("namespace"),
        keywordField("path")
      ),
      keywordField("type")
    )

  private def manifestFields(name: String): ObjectField =
    objectField(name).fields(
      objectField("checksumAlgorithm").fields(
        keywordField("type")
      ),
      nestedField("files").fields(
        keywordField("checksum"),
        keywordFieldWithText("name"),
        keywordField("path"),
        longField("size")
      )
    )

  val mapping: MappingDefinition = properties(
    keywordField("space"),
    infoFields,
    intField("version"),
    manifestFields("manifest"),
    manifestFields("tagManifest"),
    locationFields("location"),
    locationFields("replicaLocations"),
    dateField("createdDate"),
    keywordField("ingestId")
  )
}

case object FilesIndexConfig extends IndexConfig {
  val mapping: MappingDefinition = properties(
    keywordField("bucket"),
    keywordFieldWithText("path"),
    keywordFieldWithText("name"),
    dateField("createdDate"),
    objectField("checksum").fields(
      keywordField("algorithm"),
      keywordField("value")
    ),
    longField("size"),
    // A single file will always be in the same space/externalIdentifier, but
    // might appear in multiple versions of that bag.
    objectField("bag").fields(
      keywordField("space"),
      keywordFieldWithText("externalIdentifier"),
      keywordField("versions")
    )
  )
}
