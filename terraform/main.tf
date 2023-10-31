# The terraform state is stored in an S3 bucket which is not created here. It was created before
# and shouldn't be touched.
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