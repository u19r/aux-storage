output "hosts" {
  value = {
    instances = { for role, host in aws_instance.host : role => {
      instance_id = host.id
      public_ip   = host.public_ip
      private_ip  = host.private_ip
    } }
    availability_zone = var.availability_zone
    ami_id            = var.ami_id
    instance_type     = local.instance_type
  }
}
