# Technologies

This document provides a brief overview of some of the key technologies we use in the storage service.

## AWS services

<table>
  <tr>
    <td>
      <img src="./images/Amazon-Simple-Storage-Service_64@5x.png">
    </td>
    <td>
      <strong>S3</strong> – petabyte-scale object storage.
      We use it for permanent storage of assets, temporary/working storage, and to hold storage "manifests" (our JSON representation of a bag).
    </td>
  </tr>
  <tr>
    <td>
      <img src="./images/Arch_Amazon-DynamoDB_64@5x.png">
    </td>
    <td>
      <strong>DynamoDB</strong> – a NoSQL database that we use as a key-value store.
      We use it to track ingests, store information about versions, and lock around certain processes.
    </td>
  </tr>
  <tr>
    <td>
      <img src="./images/Arch_Amazon-Simple-Queue-Service_64@5x.png">
      <img src="./images/Arch_Amazon-Simple-Notification-Service_64@5x.png">
    </td>
    <td>
      <strong>SNS/SQS</strong> – inter-app message queues.
      Our apps form a pipeline: an app gets a message from an SQS queue, does some work, then sends an SNS notification to the next app.<br/><br/>
      We use SNS <em>topics</em> and SQS <em>queues</em>.
      Queues can subscribe to the output of topics, in a many-to-many relationship.
      This allows one app to send messages to multiple apps, or one app to receive messages from multiple apps.
    </td>
  </tr>
  <tr>
    <td>
      <img src="./images/Arch_AWS-Fargate_64@5x.png">
    </td>
    <td>
      <strong>ECS/Fargate</strong> – a serverless container runtime.
      We package our services in Docker images, and then Fargate actually runs the containers, without us having to provision VMs/servers to run them on.
      Fargate is a subservice of Elastic Container Service, or ECS.
    </td>
  </tr>
</table>

## Tools

<table>
  <tr>
    <td>
      <img src="./images/scala_logo.png">
    </td>
    <td>
      <strong>Scala</strong> – a JVM-based language with an emphasis on functional programming.
      Most of the storage service applications are written in Scala.
    </td>
  </tr>
  <tr>
    <td>
      <img src="./images/terraform_logo.png">
    </td>
    <td>
      <strong>Terraform</strong> – an infrastructure-as-code tool that we use to manage our resources (AWS services, Elastic Cloud clusters, Azure storage containers, and so on).
    </td>
  </tr>
  <tr>
    <td>
      <img src="./images/python_logo.png">
    </td>
    <td>
      <strong>Python</strong> – a scripting language that we use as "glue" code between certain applications, and for local debugging scripts.
    </td>
  </tr>
</table>
