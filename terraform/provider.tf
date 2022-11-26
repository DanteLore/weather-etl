provider "aws" {
  region = "eu-west-1"
}

terraform {
  backend "s3" {
    bucket         = "dantelore.tfstate"
    key            = "weather.tfstate"
    region         = "eu-west-1"
  }
}
