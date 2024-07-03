module "ingest_inspector" {
  source = "./ingest_inspector"

  domain_name                = "ingest-inspector.wellcomecollection.org"
  serve_frontend_bucket_name = "wellcomecollection-ingest-inspector-frontend"
}
