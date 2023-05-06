# Viewing application logs

In the Wellcome installation, all the apps write their logs to our shared logging cluster.
You can use these links to jump to a pre-filtered search for storage service logs:

*   <a href="https://logging.wellcomecollection.org/app/discover#/?_g=(filters:!(),refreshInterval:(pause:!t,value:0),time:(from:now-3h,to:now))&_a=(columns:!(service_name,log),filters:!(('$state':(store:appState),meta:(alias:!n,disabled:!f,index:cb5ba262-ec15-46e3-a4c5-5668d65fe21f,key:ecs_cluster,negate:!f,params:(query:storage-prod),type:phrase),query:(match_phrase:(ecs_cluster:storage-prod)))),grid:(columns:(service_name:(width:255.5))),index:cb5ba262-ec15-46e3-a4c5-5668d65fe21f,interval:auto,query:(language:kuery,query:''),sort:!(!('@timestamp',desc)))">in prod</a>
*   <a href="https://logging.wellcomecollection.org/app/discover#/?_g=(filters:!(),refreshInterval:(pause:!t,value:0),time:(from:now-3h,to:now))&_a=(columns:!(service_name,log),filters:!(('$state':(store:appState),meta:(alias:!n,disabled:!f,index:cb5ba262-ec15-46e3-a4c5-5668d65fe21f,key:ecs_cluster,negate:!f,params:(query:storage-staging),type:phrase),query:(match_phrase:(ecs_cluster:storage-staging)))),grid:(columns:(service_name:(width:255.5))),index:cb5ba262-ec15-46e3-a4c5-5668d65fe21f,interval:auto,query:(language:kuery,query:''),sort:!(!('@timestamp',desc)))">in staging</a>

{% hint style="info" %}
The indexer apps are *extremely* chatty (despite several attempts to make them quieter); the first step of searching the logs is usually to exclude those apps.
{% endhint %}
