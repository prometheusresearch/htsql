#/bin/sh

# Post-installation script for the mysql51 VM.

# Update the hostname.
echo mysql51-vm >/etc/hostname

# Preset the password for the MySQL root user.
echo "mysql-server-5.1 mysql-server/root_password password admin" | debconf-set-selections
echo "mysql-server-5.1 mysql-server/root_password_again password admin" | debconf-set-selections

# Install MySQL 5.1.
apt-get -y install mysql-server-5.1

# Configure MySQL to listen on all interfaces.
cat <<END >/etc/mysql/conf.d/bind_address.cnf
[mysqld]
bind-address = 0.0.0.0
END

# Start the server (since it is not started during OS installation).
/etc/init.d/mysql start

# Grant administrative privileges to the root user regardless of the client
# hostname.
cat <<END | mysql -uroot -padmin
GRANT ALL PRIVILEGES ON *.* TO root@'%' IDENTIFIED BY 'admin' WITH GRANT OPTION;
END

