package uk.ac.wellcome.platform.archive.indexer.bag

import com.sksamuel.elastic4s.ElasticDsl._
import com.sksamuel.elastic4s.requests.mappings.{
  FieldDefinition,
  KeywordField,
  ObjectField
}
import uk.ac.wellcome.platform.archive.indexer.elasticsearch.IndexConfig

object ManifestsIndexConfig extends IndexConfig {
  protected def keywordFieldWithText(name: String): KeywordField =
    keywordField(name).fields(textField("text"))

  private val displayInfoFields: Seq[FieldDefinition] = Seq(
    keywordFieldWithText("externalIdentifier"),
    keywordField("payloadOxum"),
    dateField("baggingDate"),
    keywordField("sourceOrganization"),
    keywordField("externalDescription"),
    keywordField("internalSenderIdentifier"),
    keywordField("internalSenderDescription"),
    keywordField("type")
  )

  private def displayManifestFields(name: String): ObjectField =
    objectField(name).fields(
      keywordField("checksumAlgorithm"),
      objectField("files").fields(
        keywordField("checksum"),
        keywordFieldWithText("name"),
        keywordField("path"),
        longField("size"),
        keywordField("type")
      ),
      keywordField("type")
    )

  override protected val fields: Seq[FieldDefinition] =
    Seq(
      keywordFieldWithText("id"),
      objectField("space").fields(displaySpaceFields),
      objectField("info").fields(displayInfoFields),
      displayManifestFields("manifest"),
      displayManifestFields("tagManifest"),
      objectField("location").fields(displayLocationFields),
      objectField("replicaLocations").fields(displayLocationFields),
      dateField("createdDate"),
      keywordField("version"),
      keywordField("type")
    )
}
