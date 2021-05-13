package uk.ac.wellcome.platform.archive.common.storage.models

import uk.ac.wellcome.storage.TypedStringScanamoOps
import weco.json.TypedString

class StorageSpace(val underlying: String) extends TypedString[StorageSpace] {
  require(underlying.nonEmpty, "Storage space cannot be empty")

  // At various points in the pipeline, we combine the storage space and
  // the external identifier into a bag ID, for example:
  //
  //    space = "digitised"
  //    identifier = "b12345678"
  //     => bag ID = "digitised/b12345678"
  //
  // We allow slashes in the IDs (for example, to match CALM IDs like PP/MIA/1).
  // To avoid ambiguity when looking at a bag ID, we forbid slashes in
  // the external identifier.
  //
  // For example, this allows us to say unambiguously that
  //
  //    bag ID = "alfa/bravo/charlie"
  //     => space = "alfa"
  //        identifier = "bravo/charlie"
  //
  // If both the space and external identifier could contain slashes,
  // this bag ID would be ambiguous.
  require(
    !underlying.contains("/"),
    s"Storage space cannot contain slashes, but got $underlying"
  )
}

object StorageSpace extends TypedStringScanamoOps[StorageSpace] {
  def apply(underlying: String): StorageSpace =
    new StorageSpace(underlying)
}
