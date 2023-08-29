package weco.storage_service.bagit.models

import weco.storage.TypedStringScanamoOps
import weco.json.TypedString

class ExternalIdentifier(val underlying: String)
    extends TypedString[ExternalIdentifier] {
  require(underlying.nonEmpty, "External identifier cannot be empty")

  // When you want to see all versions of a bag in the bags API, you call
  //
  //    GET /bags/{space}/{externalIdentifier}/versions
  //
  // To avoid ambiguity, block callers from creating an identifier that
  // ends with /versions.
  require(
    !underlying.endsWith("/versions"),
    "External identifier cannot end with /versions"
  )

  // When we store a bag in S3, we store different versions of it under the key
  //
  //    s3://{bucket}/{space}/{externalIdentifier}/v1
  //                                              /v2
  //                                              /v3
  //
  // To avoid confusion when browsing S3, block callers from creating an
  // identifier that includes anything that looks like /v1, /v2, etc.
  require(
    !underlying.matches("^.*/v\\d+$"),
    "External identifier cannot end with a version string"
  )

  require(
    !underlying.matches("^.*/v\\d+/.*$"),
    "External identifier cannot contain a version string"
  )

  require(
    !underlying.matches("^v\\d+/.*$"),
    "External identifier cannot start with a version string"
  )

  // If you put a slash at the end of the identifier (e.g. "b12345678/"), you'd
  // get an S3 key like:
  //
  //    s3://{bucket}/{space}/b12345678//v1
  //
  // The S3 Console is liable to do weird things if you have a double slash in
  // the key, so prevent people from putting slashes at the beginning or end.
  require(
    !underlying.startsWith("/"),
    "External identifier cannot start with a slash"
  )
  require(
    !underlying.endsWith("/"),
    "External identifier cannot end with a slash"
  )
  require(
    !underlying.contains("//"),
    "External identifier cannot contain consecutive slashes"
  )

  // We're super careful about the characters we allow in external identifiers,
  // because anything we use will end up in a URL for the bags API:
  //
  //    /bags/{space}/{externalIdentifier}
  //
  // It's easier to block characters that don't URL-encode well rather than
  // trying to handle the mess that is URL-encoding.
  //
  // If we need to revisit this, we should consider rethinking the bags API.
  //
  // Use cases for specific characters beyond alphanumeric:
  //
  //    - We use forward slashes for CALM identifiers, e.g. `ARTCOO/B/14`
  //      These form a hierarchy in CALM, which we can replicate in the storage service.
  //
  //    - We use spaces in the bags of Miro images, e.g. `B images`
  //
  val regex: String = "^[a-zA-Z0-9_\\-/ ]+$"
  require(
    underlying.matches(regex),
    s"External identifier must match regex: $regex"
  )
}

object ExternalIdentifier extends TypedStringScanamoOps[ExternalIdentifier] {
  def apply(underlying: String): ExternalIdentifier =
    new ExternalIdentifier(underlying)
}
