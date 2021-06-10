package uk.ac.wellcome.platform.archive.indexer.bags

import com.sksamuel.elastic4s.ElasticDsl._
import com.sksamuel.elastic4s.fields.ElasticField
import uk.ac.wellcome.platform.archive.indexer.elasticsearch.StorageServiceIndexConfig

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
      objectField("info").fields(infoFields),
      objectField("location").fields(locationFields),
      objectField("replicaLocations").fields(locationFields),
      intField("filesCount"),
      longField("filesTotalSize")
    )
}
