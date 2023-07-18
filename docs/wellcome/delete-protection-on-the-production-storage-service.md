# Delete protection on the production storage service

To prevent accidental deletion or modification of objects in permanent storage, we have a number of deletion protections in place:

*   **Explicit deny on our standard IAM roles.**
    Our [standard admin/developer roles][roles] give us permission to do almost everything in S3.
    We've added an explicit Deny for `DeleteObject` and `PutObject` in our permanent storage buckets, which makes those roles slightly less destructive.
    
    Where we grant cross-account access to the buckets, we only allow `GetObject` and `ListBucket`.
    
*   **We enable S3 versioning on the buckets.**
    This gives us a 30-day grace safety net against any accidental deletions.
    
*   **We don't use [S3 Object Lock].**
    This is an S3 feature meant for objects that must absolutely be retained for a given period, e.g. for legal compliance.
    We don't enable it because we occasionally need to delete material, and Object Lock would prevent us from doing so.

*   **We need to talk to D&T to unlock the Azure replicas.**
    Even if we somehow deleted both copies of an object in S3, nobody in the Platform team has write access to the Azure replica.
    We have to talk to D&T to get that access, and there's typically a delay of several days to get that approved and set up.

    Additionally, once you're in Azure, we have a temporary legal hold which has to be removed before you can delete anything.
    This is a weaker variant of S3 Object Lock.

[roles]: https://wellcome.slack.com/archives/CGXDT2GSH/p1689678055672609
[S3 Object Lock]: https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-lock.html
