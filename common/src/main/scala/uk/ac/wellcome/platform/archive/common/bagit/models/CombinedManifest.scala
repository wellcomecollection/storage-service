package uk.ac.wellcome.platform.archive.common.bagit.models

import java.io.InputStream

import uk.ac.wellcome.platform.archive.common.verify.VerifiableChecksum

import scala.util.Try

/** A BagIt bag must have at least one file manifest, but it can multiple manifests --
  * for different checksum algorithms, e.g. a bag could have manifest-sha1.txt and
  * manifest-sha256.txt.
  *
  * These classes contain the information from all the manifest files in a bag.
  *
  */
sealed trait CombinedManifest {
  val files: Map[BagPath, VerifiableChecksum]
}

case class PayloadManifest(
  files: Map[BagPath, VerifiableChecksum]
) extends CombinedManifest

case object PayloadManifest {
  def create(
    md5: Option[InputStream] = None,
    sha1: Option[InputStream] = None,
    sha256: InputStream,
    sha512: Option[InputStream] = None
  ): Try[PayloadManifest] =
    for {
      files <- CombinedManifestParser.createFileLists(
        md5 = md5,
        sha1 = sha1,
        sha256 = sha256,
        sha512 = sha512
      )
    } yield PayloadManifest(files)
}

case class TagManifest(
  files: Map[BagPath, VerifiableChecksum]
) extends CombinedManifest

case object TagManifest {
  def create(
    md5: Option[InputStream] = None,
    sha1: Option[InputStream] = None,
    sha256: InputStream,
    sha512: Option[InputStream] = None
  ): Try[TagManifest] =
    for {
      files <- CombinedManifestParser.createFileLists(
        md5 = md5,
        sha1 = sha1,
        sha256 = sha256,
        sha512 = sha512
      )
    } yield TagManifest(files)
}
