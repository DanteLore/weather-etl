terraform {
  required_version = ">= 1.6" # older versions can't read the SSO sso-session profile

  backend "s3" {
    bucket = "dantelore.tfstate"
    key    = "weather.tfstate"
    region = "eu-west-1"
  }
}
