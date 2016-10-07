#/bin/sh

# Post-installation script for the Source Package BuildBot.
set -ex

# Update the hostname.
echo "src-vm" >/etc/hostname

# Update the list of packages.
apt-get -q update

# Install Mercurial.
apt-get -qy install mercurial

# Install Python 2.7 and required Python packages.
apt-get -qy install python2.7
apt-get -qy install python-setuptools
apt-get -qy install python-pip
apt-get -qy install python-yaml

# Install sphinxcontrib-texfigure dependencies.
apt-get -qy install pgf
apt-get -qy install poppler-utils
apt-get -qy install netpbm

# Install Sphinx and custom Sphinx extensions.
pip -q install Sphinx
pip -q install sphinxcontrib-htsql
pip -q install sphinxcontrib-texfigure

# Normalize the name of `rst2*` scripts.
for filename in /usr/local/bin/rst2*.py; do
    ln -s $filename ${filename%.*};
done

# Clean APT cache.
apt-get clean

