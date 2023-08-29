# Why did my callback to Goobi return a 401 Unauthorized?

When Goobi sends a bag to the storage service, it includes a callback URL.
This URL is called by the notifier when the bag is successfully stored.

If a bag takes a long time to ingest (or especially if it gets stuck), the notifier can return an error:

> Callback failed for: [ingest ID], got 401 Unauthorized!

This is because Goobi's callback URLs are only valid for three days.
If a bag takes more than three days to store, the callback URL will expire.

This isn't a major issue – the bag will still be stored correctly, but somebody will need to manually advance it in Goobi.
