# -*- encoding: utf-8


def test_refreshes_an_expired_token(client):
    client.get_ingest("025a929b-7ec4-4fe9-836a-a65b39528b09")

    original_token = client.sess.token["access_token"]

    # Fiddle the expiry time on the session token so it appears to have
    # expired two hours ago.
    client.sess.token["expires_at"] -= 7200

    # Now make a second request, which should trigger a token refresh.
    client.get_ingest("025a929b-7ec4-4fe9-836a-a65b39528b09")

    assert client.sess.token["access_token"] != original_token
