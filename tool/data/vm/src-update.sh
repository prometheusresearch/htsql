#/bin/sh

# Post-installation script for the Source Package BuildBot.
set -ex

# Update the hostname.
echo "src-vm" >/etc/hostname

# Install Mercurial.
apt-get -qy install mercurial

# Install Python 2.6 and required Python packages.
apt-get -qy install python2.6
apt-get -qy install python-setuptools
apt-get -qy install python-yaml

# Documentation build dependencies.
apt-get -qy install -t squeeze-backports python-sphinx
apt-get -qy install pgf
apt-get -qy install poppler-utils
apt-get -qy install netpbm

# Clean APT cache.
apt-get clean

