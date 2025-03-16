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

resource "aws_s3_bucket" "utd19_bucket" {
  bucket = "utd19"
  force_destroy = true

  tags = {
    Name        = "UTD19 data"
  }
}