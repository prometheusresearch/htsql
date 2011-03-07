#/bin/sh

echo py25-vm >/etc/hostname

echo "deb http://oss.oracle.com/debian/ unstable main non-free" >/etc/apt/sources.list.d/oracle.list
wget -q http://oss.oracle.com/el4/RPM-GPG-KEY-oracle -O- | apt-key add -
apt-get update

apt-get -y install mercurial

apt-get -y install python2.5
apt-get -y install python-setuptools
apt-get -y install python-yaml
apt-get -y install python-pip
apt-get -y install python-virtualenv

apt-get -y install python2.5-dev

apt-get -y install libsqlite3-dev
apt-get -y install libpq-dev
apt-get -y install libmysqlclient-dev
apt-get -y install freetds-dev
apt-get -y install oracle-xe-client

virtualenv -p python2.5 .

mkdir src
hg -q clone https://bitbucket.org/prometheus/htsql src/htsql

cat <<END >>/root/.bashrc

export PATH=~/bin:\$PATH
export LD_LIBRARY_PATH=~/lib

export ORACLE_HOME=/usr/lib/oracle/xe/app/oracle/product/10.2.0/client
export NLS_LANG=AMERICAN_AMERICA.AL32UTF8
export SQLPATH=\$ORACLE_HOME/sqlplus
export PATH=\$PATH:\$ORACLE_HOME/bin
export LD_LIBRARY_PATH=\$LD_LIBRARY_PATH:\$ORACLE_HOME/lib

END

#cat <<END >>/root/.bashrc
#
#. /usr/lib/oracle/xe/app/oracle/product/10.2.0/client/bin/oracle_env.sh
#END

#. /usr/lib/oracle/xe/app/oracle/product/10.2.0/client/bin/oracle_env.sh

#python2.5 -mpip install pysqlite
#python2.5 -mpip install psycopg2
#python2.5 -mpip install mysql-python
#python2.5 -mpip install pymssql -f http://pypi.python.org/pypi/pymssql/ --no-index
#python2.5 -mpip install cx-oracle

#python2.5 -c 'import pysqlite2, psycopg2, MySQLdb, pymssql, cx_Oracle'

#apt-get -qq install python-pysqlite2 >/dev/null
#apt-get -qq install python-psycopg2 >/dev/null
#apt-get -qq install python-mysqldb >/dev/null


