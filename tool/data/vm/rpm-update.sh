#/bin/sh

# Post-installation script for the Centos RPM Build.
set -ex

# Update the hostname.
echo "rpm-vm" >/etc/hostname
