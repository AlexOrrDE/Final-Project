provider "aws" {
    region = "eu-west-2"
}

# This bit breaks it. What was it actually for?
# terraform{
#     backend "s3" {
#         bucket = "de-day1-iulia"
#         key = "terraform.tfstate"
#         region = "eu-west-2"
#     }
# }