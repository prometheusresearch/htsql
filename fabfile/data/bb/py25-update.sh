#/bin/sh

# Post-installation script for the py25 VM.
set -ex

# Update the hostname.
echo py25-vm >/etc/hostname

# Register the Oracle repository.
echo "deb http://oss.oracle.com/debian/ unstable main non-free" >/etc/apt/sources.list.d/oracle.list
wget -q http://oss.oracle.com/el4/RPM-GPG-KEY-oracle -O- | apt-key add -
apt-get -q update

# Install Mercurial.
apt-get -qy install mercurial

# Install Python 2.5 and required Python packages.
apt-get -qy install python2.5
apt-get -qy install python-setuptools
apt-get -qy install python-yaml
apt-get -qy install python-pip
apt-get -qy install python-virtualenv

# Install development files for Python and database drivers.
apt-get -qy install python2.5-dev
apt-get -qy install libpq-dev
apt-get -qy install libmysqlclient-dev
apt-get -qy install freetds-dev
apt-get -qy install oracle-xe-client

# Clean APT cache.
apt-get clean

# Initialize Python virtual enviroment in `/root`.
virtualenv -p python2.5 .

# Set the Oracle, FreeTDS and `virtualenv` environment variables on login.
cat <<END >>/root/.bashrc

export PATH=~/bin:\$PATH
export LD_LIBRARY_PATH=~/lib

export ORACLE_HOME=/usr/lib/oracle/xe/app/oracle/product/10.2.0/client
export NLS_LANG=AMERICAN_AMERICA.AL32UTF8
export SQLPATH=\$ORACLE_HOME/sqlplus
export PATH=\$PATH:\$ORACLE_HOME/bin
export LD_LIBRARY_PATH=\$LD_LIBRARY_PATH:\$ORACLE_HOME/lib

export TDSVER=8.0

END

