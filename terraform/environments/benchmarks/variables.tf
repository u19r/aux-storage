variable "campaign" {
  type = string
  validation {
    condition     = can(regex("^benchmark-[a-z0-9-]+$", var.campaign))
    error_message = "Use a benchmark-prefixed campaign identifier."
  }
}

variable "backend" {
  type = string
  validation {
    condition     = contains(["postgres", "foundationdb", "extenddb", "dynoxide"], var.backend)
    error_message = "Select a backend in the comparison campaign."
  }
}

variable "runner_run" {
  type        = string
  description = "Change for every split or retry to replace the runner instance."
}

variable "build_only" {
  type        = bool
  default     = false
  description = "Use a disposable Spot builder before creating fresh measurement hosts."
}

variable "availability_zone" {
  type    = string
  default = "eu-north-1a"
  validation {
    condition     = can(regex("^eu-north-1[a-z]$", var.availability_zone))
    error_message = "The availability zone must belong to eu-north-1."
  }
}

variable "ssh_cidr" {
  type = string
  validation {
    condition     = can(cidrhost(var.ssh_cidr, 0)) && endswith(var.ssh_cidr, "/32")
    error_message = "Restrict SSH to one operator IPv4 address."
  }
}

variable "ssh_public_key_path" {
  type    = string
  default = "~/.ssh/id_ed25519.pub"
}

variable "ami_id" {
  type        = string
  description = "Immutable ARM64 Ubuntu AMI shared by the entire campaign."
  validation {
    condition     = can(regex("^ami-[0-9a-f]+$", var.ami_id))
    error_message = "Resolve and pin one AMI before launching the campaign."
  }
}
