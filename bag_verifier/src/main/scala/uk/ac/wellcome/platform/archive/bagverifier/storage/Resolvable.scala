package uk.ac.wellcome.platform.archive.bagverifier.storage

import java.net.URI

trait Resolvable[T] {
  def resolve(t: T): URI
}
