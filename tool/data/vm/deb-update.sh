#/bin/sh

# Post-installation script for the Debian DEB Build.
set -ex

# Update the hostname.
echo "deb-vm" >/etc/hostname

apt-get -qy install python-all
apt-get -qy install debhelper
apt-get -qy install python-setuptools
apt-get -qy install python-yaml

apt-get clean
