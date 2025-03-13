variable "aws_profile" {
  description = "AWS profile to manage resources"
  default     = "default"
}

variable "credentials" {
  description = "Credentials"
  default     = "~/.aws/credentials"
}

variable "region" {
  description = "Project region"
  default     = "eu-central-1"
}