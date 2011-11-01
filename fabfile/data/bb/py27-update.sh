#/bin/sh

# Post-installation script for the py27 VM.

# Update the hostname.
echo py27-vm >/etc/hostname

# Register the testing repository.
echo "deb http://ftp.us.debian.org/debian/ wheezy main" >/etc/apt/sources.list.d/testing.list
cat <<END >>/etc/apt/preferences.d/pinning.pref
Package: *
Pin: release n=wheezy
Pin-Priority: 90
END
apt-get update

# Register the Oracle repository.
echo "deb http://oss.oracle.com/debian/ unstable main non-free" >/etc/apt/sources.list.d/oracle.list
wget -q http://oss.oracle.com/el4/RPM-GPG-KEY-oracle -O- | apt-key add -
apt-get update

# Install Mercurial.
apt-get -y install mercurial

# Install Python 2.7 and required Python packages.
apt-get -y install -t wheezy python2.7
apt-get -y install -t wheezy python-setuptools
apt-get -y install -t wheezy python-yaml
apt-get -y install -t wheezy python-pip
apt-get -y install -t wheezy python-virtualenv

# Install development files for Python and database drivers.
apt-get -y install -t wheezy python2.7-dev
apt-get -y install libpq-dev
apt-get -y install libmysqlclient-dev
apt-get -y install freetds-dev
apt-get -y install oracle-xe-client

# Initialize Python virtual enviroment in `/root`.
virtualenv -p python2.7 .

# Download the source code of HTSQL.
mkdir src
hg -q clone https://bitbucket.org/prometheus/htsql src/htsql

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

