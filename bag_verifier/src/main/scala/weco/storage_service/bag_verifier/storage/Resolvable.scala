package weco.storage_service.bag_verifier.storage

import java.net.URI

trait Resolvable[T] {
  def resolve(t: T): URI
}
