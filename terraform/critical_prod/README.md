# critical_prod

This stack contains the Terraform definitions for all the irreplaceable data stores for the storage service, including:

*   The S3 bucket for the primary replica
*   The S3 bucket for the Glacier replica
*   The DynamoDB databases with identifiers, versions, &c

Please be careful when working in this stack!



## Known issues and workarounds

### AccessDenied when changing the S3 bucket policies

If you're trying to change the S3 bucket policy, you may see this error when you apply your Terraform change:

```
Error: Error putting S3 policy: AccessDenied: Access Denied
```

This is because, by default, we disable `s3:Put*` and `s3:Delete*` on both S3 buckets (to avoid accidental corruption of the archive).

These protections are defined in `delete_protection.tf` -- temporarily comment out the line disabling `s3:PutBucket*`, then try to apply your change again.
You'll need to run Terraform twice: once to change the IAM permissions to re-enable `s3:PutBucket*`, once to update the S3 bucket policy.

*Don't forget to re-enable the protection afterwards!*

### MalformedPolicy when changing the S3 bucket policies

If you're trying to change the S3 bucket policy, you may see this error when you apply your Terraform change:

```
Error: Error putting S3 policy: MalformedPolicy: Invalid principal in policy
```

This error message is slightly misleading.
It could mean:

*   The string you provided isn't a valid IAM principal, or
*   The string you provided *looks like* a valid IAM principal, but the principal doesn't exist

When AWS tries to apply an S3 bucket policy, it checks that all the principals it contains actually exist, hence the second type of error.

Check that:

*   You haven't typo'd the name of an IAM principal
*   All of the IAM principals in the policy still exist
