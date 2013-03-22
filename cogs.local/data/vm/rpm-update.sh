#/bin/sh

# Post-installation script for the Centos RPM Build.
set -ex

# Update the hostname.
echo "rpm-vm" >/etc/hostname

# Add EPEL repository.
rpm -Uvh http://download.fedoraproject.org/pub/epel/6/i386/epel-release-6-8.noarch.rpm

# install packages
yum -q -y install rpmdevtools
yum -q -y install python-setuptools
yum -q -y install PyYAML
yum -q -y install python-devel

