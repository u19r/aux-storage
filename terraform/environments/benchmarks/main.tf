data "aws_vpc" "default" {
  default = true
}

data "aws_subnet" "default" {
  filter {
    name   = "vpc-id"
    values = [data.aws_vpc.default.id]
  }
  filter {
    name   = "availability-zone"
    values = [var.availability_zone]
  }
  filter {
    name   = "default-for-az"
    values = ["true"]
  }
}

resource "aws_security_group" "benchmark" {
  name_prefix = "${var.campaign}-"
  vpc_id      = data.aws_vpc.default.id
  ingress {
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = [var.ssh_cidr]
  }
  ingress {
    from_port = 0
    to_port   = 65535
    protocol  = "tcp"
    self      = true
  }
  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_key_pair" "benchmark" {
  key_name_prefix = "${var.campaign}-"
  public_key      = file(pathexpand(var.ssh_public_key_path))
}

locals {
  instance_type = "m8gd.4xlarge"
  hosts = var.build_only ? { builder = var.runner_run } : {
    database = var.backend
    runner   = "${var.backend}-${var.runner_run}"
  }
}

resource "terraform_data" "generation" {
  for_each = local.hosts
  # A physical SKU change must replace both Spot hosts, including the runner.
  input = "${each.value}-${local.instance_type}"
}

resource "aws_instance" "host" {
  for_each                    = local.hosts
  ami                         = var.ami_id
  instance_type               = local.instance_type
  subnet_id                   = data.aws_subnet.default.id
  vpc_security_group_ids      = [aws_security_group.benchmark.id]
  associate_public_ip_address = true
  key_name                    = aws_key_pair.benchmark.key_name
  user_data_replace_on_change = true
  user_data = each.key == "database" ? "#cloud-config\n${yamlencode({
    write_files = [{
      path        = "/etc/default/grub.d/99-benchmark-limits.cfg"
      permissions = "0644"
      # Keep a common host envelope; native and application services enforce separate budgets.
      content = "GRUB_CMDLINE_LINUX_DEFAULT=\"$GRUB_CMDLINE_LINUX_DEFAULT mem=32G maxcpus=6\"\n"
    }]
    runcmd      = [["update-grub"]]
    power_state = { mode = "reboot", delay = "now", timeout = 120, condition = true }
  })}" : null
  instance_market_options {
    market_type = "spot"
    spot_options {
      instance_interruption_behavior = "terminate"
      spot_instance_type             = "one-time"
    }
  }
  root_block_device {
    volume_size = each.key == "runner" ? 30 : 80
    volume_type = "gp3"
    encrypted   = true
  }
  metadata_options {
    http_tokens = "required"
  }
  lifecycle {
    replace_triggered_by = [terraform_data.generation[each.key]]
  }
  tags = { Name = "${var.campaign}-${var.backend}-${each.key}", Role = each.key }
}
