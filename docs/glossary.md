# Glossary

These are the naming conventions used in the codebase, particularly in variable names.

<dl>


<dt>Glacier replica/Ireland replica</dt>
<dd>
  <p>
    The cold copy of a bag kept in S3 Glacier in Ireland.
    In practice, this means the copy kept in <code>s3://wc-storage-replica-ireland</s3> or the staging copy.
  </p>

  <p>
    This used to be called the <em>archive copy</em>, although hopefully all references to that have been removed.
  </p>
</dd>


<dt>primary replica</dt>
<dd>
  <p>
    The primary/hot copy of a bag kept in the storage service.
    In practice, this means the copy kept in <code>s3://wc-storage</code> or <code>s3://wc-storage-staging</code>, in Standard IA storage.
    If somebody wants to read a file from the bag, they should read it from the primary replica.
  </p>

  <p>
    This used to be called the <em>access copy</em>, although hopefully all references to that have been removed.
  </p>
</dd>


</dl>
