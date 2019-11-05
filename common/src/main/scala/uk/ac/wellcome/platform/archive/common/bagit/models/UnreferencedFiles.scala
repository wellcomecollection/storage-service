package uk.ac.wellcome.platform.archive.common.bagit.models

/** In a bag, we have objects.  Those objects are referred to by a manifest file,
  * which is in turn referred to by a tag manifest file.  But how do we know the
  * tag manifest file is there?
  *
  *     b1234.jp2
  *     object
  *     |
  *     +---- manifest-sha256.txt
  *           manifest file
  *           |
  *           +---- tagmanifest-sha256.txt
  *                 tag manifest file
  *
  * There's nothing in a bag that refers to the tag manifest file.  This is okay --
  * the chain of checksums/references has to stop somewhere!
  *
  * This object records the filenames of all the tag manifests we might expect to see.
  *
  * The BagIt spec supports four checksum algorithms, and you can send
  * multiple manifests with different algorithms in the same bag.
  * See https://tools.ietf.org/html/rfc8493#section-2.4
  *
  */
object UnreferencedFiles {
  val tagManifestFiles: Seq[String] = Seq(
    "tagmanifest-md5.txt",
    "tagmanifest-sha1.txt",
    "tagmanifest-sha256.txt",
    "tagmanifest-sha512.txt"
  )
}
