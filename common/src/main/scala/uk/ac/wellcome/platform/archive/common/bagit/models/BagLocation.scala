package uk.ac.wellcome.platform.archive.common.bagit.models

import java.net.URI

import uk.ac.wellcome.platform.archive.common.verify.ChecksumValue

sealed trait BagLocation {
  val path: BagPath
}

case class BagFile(
                    checksum: ChecksumValue,
                    path: BagPath
                  ) extends BagLocation

case class BagFetchEntry(
                          url: URI,
                          length: Option[Int],
                          path: BagPath
                        ) extends BagLocation


object BagFetchEntry {
  def create(url: URI,
            length: Option[Int],
            path: String) = BagFetchEntry(url, length, BagPath(path))
}
