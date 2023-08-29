# Getting notifications of newly stored bags

When the storage service successfully stores a bag, it sends a notification to an SNS topic.
You can subscribe to this topic to react to updates, e.g. if you want to trigger some downstream processing.

This will be a topic whose name ends `registered_bag_notifications`.

This topic will receive JSON messages that look similar to the following:

```json
{
  "space": "digitised",
  "externalIdentifier": "b20278512",
  "version": "v1",
  "type": "RegisteredBagNotification"
}
```

Any bag described in this topic should be retrievable through the bags API.
