#! /usr/bin/env bash

# checkout tailsale installation
which tailscale >/dev/null 2>&1 || {
  echo "Tailscale is not installed. Installing..."
  curl -fsSL https://tailscale.com/install.sh | sh
}

# get permission
sudo tailscale set --operator=$USER

# connect to tailscale
tailscale up

# expose fastapi port
tailscale funnel -bg $API_PORT
