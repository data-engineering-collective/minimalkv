variable "project_id" {
  default = "qc-minimalkv"
  description = "GCP project name"
}

provider "google" {
  project = var.project_id
  region  = "europe-west3"
}

# Set up the backend bucket

resource "google_storage_bucket" "backend" {
  name = "qc-minimalkv-backend"
  versioning {
    enabled = true
  }
  location = "EU"
}

terraform {
  backend "gcs" {
    bucket  = "qc-minimalkv-backend"
    prefix  = "terraform/state"
  }
}

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

# Grant the external identities permission to impersonate the service account
resource "google_service_account_iam_binding" "qc-minimalkv-account-iam" {
  service_account_id = google_service_account.service_account_gha.name
  role = "roles/iam.workloadIdentityUser"
  members = [
    "principalSet://iam.googleapis.com/${google_iam_workload_identity_pool.github_actions.name}/attribute.repository/data-engineering-collective/minimalkv",
  ]
}

# Grant the external identities permission to create tokens for the service account
resource "google_service_account_iam_binding" "qc-minimalkv-account-iam-token-creator" {
  service_account_id = google_service_account.service_account_gha.name
  role = "roles/iam.serviceAccountTokenCreator"
  members = [
    "principalSet://iam.googleapis.com/${google_iam_workload_identity_pool.github_actions.name}/attribute.repository/data-engineering-collective/minimalkv",
  ]
}

# Grant the service account permission to access GCS buckets
resource "google_project_iam_member" "sa-storageaccess" {
  role    = "roles/storage.admin"
  member  = "serviceAccount:${google_service_account.service_account_gha.email}"
  project = "qc-minimalkv"
}