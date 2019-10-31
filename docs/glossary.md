# Glossary

These are the naming conventions used in the codebase, particularly in variable names.



## Terms in use

*   **Glacier replica/Ireland replica.**
    The cold copy of a bag kept in S3 Glacier in Ireland.
    In practice, this means the copy kept in `s3://wc-storage-replica-ireland` or the staging equivalent.

*   **Primary replica.**
    The primary/hot copy of a bag kept in the storage service.
    In practice, this means the copy kept in `s3://wc-storage` or `s3://wc-storage-staging`, in Standard IA storage.
    If somebody wants to read a file from the bag, they should read it from the primary replica.



## Terms we no longer use

This is a list of terms that we used to use in the storage service codebase, but which have been replaced/removed.
If you find references to these in current code, they should probably be updated/removed.

*   **Access/archive copies**, now the *primary* or *Glacier replicas*, respectively.

*   **Bag auditor.**
    The early name of the *bag versioner*.