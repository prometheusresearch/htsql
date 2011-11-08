#/bin/sh

# Post-installation script for CentOS VM.

# Add the public key to /root/.ssh/authorized_keys.
mkdir /root/.ssh
chmod go-rwx /root/.ssh
cp identity.pub /root/.ssh/authorized_keys
chmod u+rw,go-rwx /root/.ssh/authorized_keys

# Add EPEL repository.
rpm -Uvh http://download.fedora.redhat.com/pub/epel/6/i386/epel-release-6-5.noarch.rpm

