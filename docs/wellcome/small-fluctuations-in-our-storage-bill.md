# Small fluctuations in our storage bill

The storage service runs in a dedicated AWS account, so the account bill only reflects the cost of the storage service.

The dominant cost in this account is the Standard-IA bill, but at a glance it can look a little disconcerting. In particular, our Standard-IA bill goes down as well as up – but why would it ever go down? Why would we ever remove objects in Standard-IA? Should we be concerned about the integrity of the storage service?

No – slight fluctuations in the Standard-IA bill are a normal part of storage service operations.

<figure>
  <img src="../images/costs_graph.png">
  <figcaption>
    The shape of our storage bill in April 2023. Not drawn to scale; drawn to illustrate the general shape.
  </figcaption>
</figure>

Why?

*   When objects in the warm replica [transition to Glacier](https://app.gitbook.com/o/-LumfFcEMKx4gYXKAZTQ/s/5fJiiTl4PgHkFAzFiHc8/~/changes/1/wellcome-specific-information/our-storage-configuration/using-multiple-storage-tiers-for-cost-efficiency-a-v-tiffs), they get removed from Standard-IA. That cost moves from Standard-IA to Glacier.
*   When objects in the cold replica [transition to Glacier Deep Archive](https://app.gitbook.com/o/-LumfFcEMKx4gYXKAZTQ/s/5fJiiTl4PgHkFAzFiHc8/~/changes/1/wellcome-specific-information/our-storage-configuration/replica-configuration), they get removed from Standard-IA. That cost moves from Standard-IA to GDA.

In the graph above, you can see a drop in the Standard-IA bill at the beginning of the month – but it's accompanied by a rise in the Glacier/Deep Archive bill. A bunch of objects just got transitioned into a cheaper storage class.

A substantial drop in the Standard-IA bill may be a red flag, but small fluctuations aren't a cause for concern.

To see this with our real numbers, visit <a href="https://us-east-1.console.aws.amazon.com/cost-management/home?region=us-east-1#/cost-explorer?chartStyle=LINE&costAggregate=unBlendedCost&endDate=2023-04-30&excludeForecasting=false&filter=%5B%7B%22dimension%22:%7B%22id%22:%22Service%22,%22displayValue%22:%22Service%22%7D,%22operator%22:%22INCLUDES%22,%22values%22:%5B%7B%22value%22:%22Amazon%20Simple%20Storage%20Service%22,%22displayValue%22:%22S3%20(Simple%20Storage%20Service)%22%7D%5D%7D,%7B%22dimension%22:%7B%22id%22:%22Operation%22,%22displayValue%22:%22API%20operation%22%7D,%22operator%22:%22INCLUDES%22,%22values%22:%5B%7B%22value%22:%22%22,%22displayValue%22:%22No%20operation%22%7D,%7B%22value%22:%22StandardStorage%22,%22displayValue%22:%22StandardStorage%22%7D,%7B%22value%22:%22StandardIAStorage%22,%22displayValue%22:%22StandardIAStorage%22%7D,%7B%22value%22:%22GlacierStorage%22,%22displayValue%22:%22GlacierStorage%22%7D,%7B%22value%22:%22DeepArchiveStorage%22,%22displayValue%22:%22DeepArchiveStorage%22%7D%5D%7D%5D&futureRelativeRange=CUSTOM&granularity=Daily&groupBy=%5B%22Operation%22%5D&historicalRelativeRange=LAST_6_MONTHS&isDefault=true&reportName=Daily%20API%20operation%20costs&showOnlyUncategorized=false&showOnlyUntagged=false&startDate=2022-11-01&usageAggregate=undefined&useNormalizedUnits=false">Cost Explorer filtered to storage costs only</a>.
Notice how our StandardStorage bill similarly fluctuates (although it's a much smaller part of our bill) – this is when objects transition from Standard to Standard-IA.
