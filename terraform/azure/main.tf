provider "azurerm" {
  version = "=2.0.0"
  features {}
}

data "azurerm_resource_group" "stage" {
  name = "rg-wcollarchive-stage"
}

resource "azurerm_storage_account" "staging" {
  resource_group_name       = data.azurerm_resource_group.stage.name

  location                  = "westeurope"
  name                      = "wellcomecollectionstage2"
  account_kind              = "StorageV2"
  account_tier              = "Standard"

  # We only use Locally Redundant storage for the Azure replica because
  # this is a backup of last resort.
  account_replication_type  = "LRS"
}

resource "azurerm_storage_container" "staging_replica" {
  name                  = "wellcomecollection-storage-staging-replica-amsterdam"
  storage_account_name  = azurerm_storage_account.staging.name
  container_access_type = "private"
}
