#/bin/sh

# Post-installation script for the Centos RPM Build.
set -ex

# Update the hostname.
echo "rpm-vm" >/etc/hostname

# install packages
yum -qy install rpmdevtools
yum -qy install python-setuptools
yum -qy install PyYAML
