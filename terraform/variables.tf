variable "do_token" {
  description = "DigitalOcean API Token"
  type        = string
}

variable "region" {
  description = "DigitalOcean Region"
  type        = string
  default     = "nyc1"
}

variable "size" {
  description = "Droplet Size"
  type        = string
  default     = "s-2vcpu-2gb"
}
