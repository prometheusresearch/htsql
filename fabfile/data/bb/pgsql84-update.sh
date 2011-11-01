#/bin/sh

# Post-installation script for the pgsql84 VM.

# Update the hostname.
echo pgsql84-vm >/etc/hostname

# Install the PostgreSQL 8.4 server.
apt-get -y install postgresql-8.4

# Set the password of the user postgres to 'admin'.
su -c "psql -c \"ALTER ROLE postgres WITH PASSWORD 'admin'\"" postgres

# Configure PostgreSQL to listen on all interfaces.
cat <<END >>/etc/postgresql/8.4/main/postgresql.conf

# Listen on all available interfaces:
listen_addresses = '*'
END

# Configure PostgreSQL to allow login from the external interface.
cat <<END >>/etc/postgresql/8.4/main/pg_hba.conf

# Allow external network connections:
host all all 10.0.0.1/8 md5
END

