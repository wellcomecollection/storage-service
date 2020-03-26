# bag_indexer

We want to use the storage service to do bulk analysis of our files.
For example, we might ask questions like:

*   How many images did we digitise last month?
*   What file formats are we holding?
*   How big are the files we're storing?

The bag indexer receives notifications of newly-registered bags, and creates a record of those bags inside an Elasticsearch cluster.

We index the manifests and the files separately, so that we can query at a per-manifest or a per-file level.
