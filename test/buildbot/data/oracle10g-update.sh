#/bin/sh

# Post-installation script for the oracle10g VM.

# Update the hostname.
echo oracle10g-vm >/etc/hostname

# Register the Oracle repository.
echo "deb http://oss.oracle.com/debian/ unstable main non-free" >/etc/apt/sources.list.d/oracle.list
wget -q http://oss.oracle.com/el4/RPM-GPG-KEY-oracle -O- | apt-key add -
apt-get update

# Install the Oracle 10g Express Edition.
apt-get -y install oracle-xe-universal

# Fix the problem when the configuration script eats the last
# character of the password if it is 'n': replace IFS="\n" with IFS=$'\n'.
sed -i -e s/IFS=\"\\\\n\"/IFS=\$\'\\\\n\'/ /etc/init.d/oracle-xe

# Configure the server; provide the answers for the following questions:
# The HTTP port for Oracle Application Express: 8080
# A port for the database listener: 1521
# The password for the SYS and SYSTEM database accounts: admin
# Start the server on boot: yes
/etc/init.d/oracle-xe configure <<END
8080
1521
admin
admin
y
END

# Set Oracle environment variables on login.
cat <<END >>/root/.bashrc

. /usr/lib/oracle/xe/app/oracle/product/10.2.0/server/bin/oracle_env.sh
END

