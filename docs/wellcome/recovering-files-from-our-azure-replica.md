# Recovering files from our Azure replica

1.  Retrieve the appropriate connection string from Secrets Manager; search for [secrets whose name contains 'azure'][secrets].

    There are read-write and read-only connection strings for staging and prod; choose the read-only string.

    You want everything in the URL query parameter, which is the SAS token.
    For example, if the URL was:

    ```
    https://wecostorageprod.blob.core.windows.net/?se=3020-01-01T01%3A01%3A01Z&sp=rl&sip=1.2.3.4&sv=2019-12-12&sr=c&sig=SIGNATURE1234
    ```

    then the token is

    ```
    se=3020-01-01T01%3A01%3A01Z&sp=rl&sip=1.2.3.4&sv=2019-12-12&sr=c&sig=SIGNATURE1234
    ```

    These tokens are IP-restricted, and can only be used from the Elastic IP in the storage account.

    [secrets]: https://eu-west-1.console.aws.amazon.com/secretsmanager/listsecrets?region=eu-west-1&search=all%3Dazure

2.  Create an EC2 host which is behind the Elastic IP address used by the storage account.
    You may need to SSH through a publicly-accessible bastion host.

    You can check your current IP address by running:

    ```console
    $ curl ifconfig.me
    ```

    and you can see our Elastic IP address [in the AWS console](https://eu-west-1.console.aws.amazon.com/ec2/home?region=eu-west-1#Addresses:).

3.  Start a Docker container running the Azure CLI:

    ```console
    $ docker run -it mcr.microsoft.com/azure-cli:2.44.1
    ```

    (You may need to install Docker on the EC2 instance first.)

4.  You can now run `az storage` commands, passing the SAS token.
    For example, to list all the blobs in a prefix:

    ```
    az storage blob directory list \
      --container-name wellcomecollection-storage-replica-netherlands \
      --directory-path digitised/b13135934/v1 \
      --account-name wecostorageprod \
      --sas-token '[SAS_TOKEN]'
    ```

    You can download an individual blob:

    ```
    az storage blob directory download \
      --container wellcomecollection-storage-replica-netherlands \
      --destination-path ./bag-info.txt \
      --source-path digitised/b32868261/v1/bag-info.txt \
      --account-name wecostorageprod \
      --sas-token '[SAS_TOKEN]'
    ```

    You can also download an entire directory:

    ```
    az storage blob directory download \
      --container wellcomecollection-storage-replica-netherlands \
      --destination-path ./b32868261 \
      --source-path digitised/b32868261/v1 \
      --account-name wecostorageprod \
      --recursive \
      --sas-token '[SAS_TOKEN]'
    ```

    (See docs for [az storage blob directory download](https://learn.microsoft.com/en-us/cli/azure/storage/blob/directory?view=azure-cli-latest#az-storage-blob-directory-download))

    This may fail if a blob has already been moved to the archive tier, with the following error in the logs:

    ```
    409 This operation is not permitted on an archived blob.. When Downloading response body.
    ```

    If so, you need to [rehydrate the blob](https://learn.microsoft.com/en-us/azure/storage/blobs/archive-rehydrate-to-online-tier?tabs=azure-cli) first.
    You can rehydrate a prefix with the following command:

    ```
    az storage blob directory list \
      --container-name wellcomecollection-storage-replica-netherlands \
      --directory-path digitised/b13135934/v1 \
      --account-name wecostorageprod \
      --sas-token '[READ_ONLY_SAS_TOKEN]' \
      | jq -r '.[].name' \
      | xargs -P 10 -I '{}' \
      az storage blob set-tier \
        --container-name wellcomecollection-storage-replica-netherlands \
        --name '{}' \
        --tier cool \
        --account-name wecostorageprod \
        --sas-token '[READ_WRITE_SAS_TOKEN]'
    ```

    Note: you will need the read-write SAS token for the `set-tier` command.

    The blobs may take up to 15 hours to hydrate, at which point you should be able to download them again.
