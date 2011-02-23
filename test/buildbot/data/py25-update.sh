#/bin/sh

echo py25-vm >/etc/hostname

apt-get -qq install mercurial >/dev/null

apt-get -qq install python2.5 >/dev/null
apt-get -qq install python-setuptools >/dev/null
apt-get -qq install python-yaml >/dev/null
apt-get -qq install python-pysqlite2 >/dev/null
apt-get -qq install python-psycopg2 >/dev/null
apt-get -qq install python-mysqldb >/dev/null

hg -q clone https://bitbucket.org/prometheus/htsql

