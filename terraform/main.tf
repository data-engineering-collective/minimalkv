variable "project_id" {
  default = "qc-minimalkv"
  description = "GCP project name"
}

# General Project Setup

#data "google_billing_account" "acct" {
#  display_name = "QuantCo"
#  open         = true
#}

provider "google" {
  project = "${var.project_id}"
  region  = "europe-west3"
}

#resource "google_project" "qc-minimalkv" {
#  name       = "${var.project_id}"
#  project_id = "${var.project_id}"
#  org_id     = "945985937868"
#
##  billing_account = data.google_billing_account.acct.id
#}

# Workload Identify Federation with Github Actions

resource "google_iam_workload_identity_pool" "github_actions" {
  workload_identity_pool_id = "${var.project_id}-gh-actions-pool"
  display_name              = "Github Actions Pool"
  description               = "Identity pool for Github Actions"
}

resource "google_iam_workload_identity_pool_provider" "github_actions_provider" {
  workload_identity_pool_id          = google_iam_workload_identity_pool.github_actions.workload_identity_pool_id
  workload_identity_pool_provider_id = "github-actions-provider"
  display_name = "GHA Identity pool provider"
  attribute_mapping = {
    "google.subject" = "assertion.sub"
    "attribute.actor" = "assertion.actor"
    "attribute.aud" = "assertion.aud"
    "attribute.repository" = "assertion.repository"
  }
  oidc {
    issuer_uri = "https://token.actions.githubusercontent.com"
  }
}

resource "google_service_account" "service_account_gha" {
  account_id   = "sa-github-actions"
  display_name = "GHA Service Accont"
}

resource "google_service_account_iam_binding" "qc-minimalkv-account-iam" {
  service_account_id = google_service_account.service_account_gha.name
  role = "roles/iam.workloadIdentityUser"
  members = [
    "principalSet://iam.googleapis.com/${google_iam_workload_identity_pool.github_actions.name}/attribute.repository/data-engineering-collective/minimalkv"
  ]
}
