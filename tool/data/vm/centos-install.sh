#/bin/sh

# Post-installation script for CentOS VM.
set -ex

# Add the public key to /root/.ssh/authorized_keys.
mkdir /root/.ssh
chmod go-rwx /root/.ssh
cp identity.pub /root/.ssh/authorized_keys
chmod u+rw,go-rwx /root/.ssh/authorized_keys

