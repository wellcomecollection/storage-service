# How to run the METS sync

1.  Mount the file share with the METS on your Mac.

    -   In Finder, select the menu item *Go* > *Connect to Server…*
    -   Connect to the server `smb://wellcomeit.com/Shares/`
    -   When the share mounts, open `LIB_WDL_DDS`

2.  Run the following command to sync the METS files to AWS:

    ```shell
    aws s3 sync \
        /Volumes/Shares/LIB_WDL_DDS/LIB_WDL_DDS_METS \
        s3://wellcomecollection-assets-workingstorage/mets \
        --storage-class STANDARD_IA \
        --exclude "*" \
        --include "*.xml" \
        --delete
    ```

    This copies all the XML files from `LIB_WDL_DDS_METS` and uploads them to the S3 bucket.
    It also deletes from S3 any files which have been deleted in the local file share.

    This takes around two days.

    You can copy a subset by adding prefixes, e.g.

    ```shell
    aws s3 sync \
        /Volumes/Shares/LIB_WDL_DDS/LIB_WDL_DDS_METS/1/2/3/4 \
        s3://wellcomecollection-assets-workingstorage/mets/1/2/3/4 \
        ...
    ```

3.  We just want the METS files for the bagger, so copy those across separately:

    ```shell
    aws s3 sync \
        s3://wellcomecollection-assets-workingstorage/mets \
        s3://wellcomecollection-assets-workingstorage/mets_only \
        --exclude "*_alto*"
    ```

    Because this is only inside S3, it's usually a lot faster.
