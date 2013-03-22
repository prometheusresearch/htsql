#/bin/sh

# Post-installation script for the Debian DEB BuildBot.
set -ex

# Update the hostname.
echo "deb-vm" >/etc/hostname

# Register the testing repository.
echo "deb http://ftp.us.debian.org/debian/ wheezy main" >/etc/apt/sources.list.d/testing.list
cat <<END >>/etc/apt/preferences.d/pinning.pref
Package: *
Pin: release n=wheezy
Pin-Priority: 90
END
apt-get -q update

# Install Python 2.7 dependencies from testing.
APT_LISTCHANGES_FRONTEND=none \
DEBIAN_FRONTEND=noninteractive \
apt-get -qy install -t wheezy gcc-4.4

# Install Python 2.7 and other build dependencies.
apt-get -qy install -t wheezy python-all
apt-get -qy install -t wheezy python-setuptools
apt-get -qy install -t wheezy python-yaml
apt-get -qy install debhelper

# Clean APT cache.
apt-get clean

