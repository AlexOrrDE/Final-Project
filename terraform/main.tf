terraform {
  required_providers {
    aws = {
        source = "hashicorp/aws"
    }
  }
    backend "s3" {
        bucket = "marble-terraform-stat"
        key = "terraform.tfstate"
        region = "eu-west-2"
}
}
provider "aws" {
    region = "eu-west-2"
}