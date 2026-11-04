terraform {
  required_version = ">= 1.8.0"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.0"
    }
  }
}

provider "aws" {
  profile = "dev-poweruser"
  region  = "eu-north-1"
  default_tags {
    tags = {
      Project  = "aux-storage-benchmark-comparison"
      Campaign = var.campaign
    }
  }
}
