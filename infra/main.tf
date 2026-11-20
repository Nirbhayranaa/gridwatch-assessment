terraform {
  required_version = ">= 1.12, < 2.0"
  required_providers {
    postgresql = {
      source  = "cyrilgdn/postgresql"
      version = "~> 1.25"
    }
  }
}

variable "db_host" {
  type = string
}
variable "admin_password" {
  type      = string
  sensitive = true
}
variable "app_password" {
  type      = string
  sensitive = true
}

provider "postgresql" {
  host            = var.db_host
  port            = 5432
  database        = "postgres"
  username        = "postgres"
  password        = var.admin_password
  sslmode         = "disable"
  connect_timeout = 10
}

resource "postgresql_role" "app" {
  name     = "gridwatch_app"
  login    = true
  password = var.app_password
}

resource "postgresql_database" "app" {
  for_each = toset(["gridwatch", "gridwatch_test"])
  name     = each.value
  owner    = postgresql_role.app.name
  lifecycle {
    prevent_destroy = true
  }
}
