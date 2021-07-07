# How to store a bag into the storage service

This guide explains how to store a bag in the storage service.

You need:

-   A bag in [the BagIt packaging format](https://datatracker.ietf.org/doc/html/rfc8493).
    This bag should have a single External-Identifier in the [bag-info.txt](https://datatracker.ietf.org/doc/html/rfc8493#section-2.2.2).

    There is an example bag with the External-Identifier `test_bag` alongside this guide.

You need to know:

-   The API URL for your storage service instance
-   The token URL for your storage service instance
-   A client ID and secret for the storage service
-   An upload bucket for the storage service

You need to choose:

-   A storage space.
    A space is an identifier that groups bags with similar content, e.g. `digitised` or `born-digital`.

To store a bag in the storage service:

1.  If not already compressed, tar-gzip compress your BagIt bag:

    ```
    tar -czf bag.tar.gz "$BAG_DIRECTORY"
    ```

1.  Upload the bag to your uploads bucket.

    ```
    aws s3 cp "bag.tar.gz" "s3://$UPLOADS_BUCKET/$UPLOADED_BAG_KEY"
    ```

1.  Fetch an access token for the OAuth2 credentials grant:

    ```
    curl -X POST "${token_url}" \
      --data grant_type=client_credentials \
      --data client_id="$CLIENT_ID" \
      --data client_secret="$CLIENT_SECRET"
    ```

    This will return a response like:

    ```
    {"access_token":"eyJraWQi...","expires_in":3600,"token_type":"Bearer"}
    ```

    Remember the `access_token`.

1.  Send a POST request to the /ingests API to create an ingest.
    This asks the storage service to store your bag:

    ```
    curl -X POST "$API_URL/ingests" \
      --header "Authorization: $ACCESS_TOKEN" \
      --header "Content-Type: application/json" \
      --data '{
        "type": "Ingest",
        "ingestType": {"id": "create", "type": "IngestType"},
        "space": {"id": "$SPACE", "type": "Space"},
        "sourceLocation": {
          "provider": {"id": "amazon-s3", "type": "Provider"},
          "bucket": "$UPLOADS_BUCKET",
          "path": "$UPLOADED_BAG_KEY",
          "type": "Location"
        },
        "bag": {
          "info": {
            "externalIdentifier": "$EXTERNAL_IDENTIFIER",
            "type": "BagInfo"
          },
          "type": "Bag"
        }
      }'
    ```

    This returns a response like:

    ```
    {"id":"ffd3c8a3-9021-47bc-a68c-75eeaff1d4bd", ...}
    ```

    Remember the `id` -- this is the ingest ID.

1.  Use the ingest ID to query the state of the ingest:

    ```
    curl "$API_URL/ingests/$INGEST_ID" \
      --header "Authorization: $ACCESS_TOKEN"
    ```

    This will return an ingest.

    You can poll this API repeatedly to see the state of your ingest as it moves through the storage service.
