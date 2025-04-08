variable "project_id" {
  default     = "utd19-traffic-dashboard"
  description = "Project ID"
}

variable "credentials" {
  default     = "./.keys/service-account-creds.json"
  description = "Service account credential file location"
}

variable "location" {
  default     = "EU"
  description = "Project location"
}

variable "region" {
  default     = "europe-west3"
  description = "Project region"
}

variable "gcs_code_bucket" {
  default     = "utd19-traffic-dashboard-code"
  description = "Bucket for code files "
}

variable "gcs_data_bucket" {
  default     = "utd19-traffic-dashboard-data"
  description = "Bucket for raw and processed data"
}