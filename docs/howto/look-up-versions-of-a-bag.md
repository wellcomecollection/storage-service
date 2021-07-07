# How to look up the versions of a bag in the storage service

This guide explains how to look up a bag which has been stored in the storage service.

You need to know:

-   The space and external identifier of the bag
-   The API URL for your storage service instance
-   The token URL for your storage service instance
-   A client ID and secret for the storage service
-   An upload bucket for the storage service

To look up a bag in the storage service:

1.  Fetch an access token for the OAuth2 credentials grant:

    ```
    curl -X POST "$TOKEN_URL" \
      --data grant_type=client_credentials \
      --data client_id="$CLIENT_ID" \
      --data client_secret="$CLIENT_SECRET"
    ```

    This will return a response like:

    ```
    {"access_token":"eyJraWQi...","expires_in":3600,"token_type":"Bearer"}
    ```

    Remember the `access_token`.

1.  Make a GET request to the /bags API, passing the space and external identiifer in the path:

    ```
    curl -X POST "$API_URL/bags/$SPACE/$EXTERNAL_IDENTIFIER/versions" \
      --header "Authorization: $ACCESS_TOKEN"
    ```

    This will return a list of versions, or a 404 error if there are no versions.
