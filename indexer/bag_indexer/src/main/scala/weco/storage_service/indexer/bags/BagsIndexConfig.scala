package weco.storage_service.indexer.bags

import com.sksamuel.elastic4s.ElasticDsl._
import com.sksamuel.elastic4s.fields.ElasticField
import weco.storage_service.indexer.elasticsearch.StorageServiceIndexConfig

object BagsIndexConfig extends StorageServiceIndexConfig {

  private val infoFields: Seq[ElasticField] = Seq(
    keywordField("externalIdentifier"),
    keywordField("payloadOxum"),
    dateField("baggingDate"),
    keywordField("sourceOrganisation"),
    keywordField("externalDescription"),
    keywordField("internalSenderIdentifier"),
    keywordField("internalSenderDescription")
  )

  private val locationFields: Seq[ElasticField] = Seq(
    keywordField("provider"),
    keywordField("bucket"),
    keywordField("path")
  )

  override protected val fields: Seq[ElasticField] =
    Seq(
      keywordField("id"),
      keywordField("space"),
      intField("version"),
      dateField("createdDate"),
      objectField("info").fields(infoFields: _*),
      objectField("location").fields(locationFields: _*),
      objectField("replicaLocations").fields(locationFields: _*),
      intField("filesCount"),
      longField("filesTotalSize")
    )
}
