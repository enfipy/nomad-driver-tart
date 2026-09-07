# Legacy upstream example: unsupported by the cloud qualification fork.
# Use the qualification contract in README.md and cloud-ctrl generated jobs.
plugin "nomad-driver-tart" {
  config {
    enabled = true
  }
}

client {
  enabled = true
  
  # Enable the tart driver
  options {
    "driver.allowlist" = "tart"
  }
}

server {
  enabled = true
  bootstrap_expect = 1
}
