provider "aws" {
  region = "eu-west-1"

  default_tags {
    tags = {
      Project    = "weather-etl"
      Repository = "https://github.com/DanteLore/weather-etl"
      ManagedBy  = "terraform"
    }
  }
}
