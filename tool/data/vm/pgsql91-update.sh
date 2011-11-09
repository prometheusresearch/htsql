#/bin/sh

# Post-installation script for the pgsql91 VM.
set -ex

# Update the hostname.
echo pgsql91-vm >/etc/hostname

# Install the PostgreSQL 9.1 server from backports.
apt-get -qy -t squeeze-backports install postgresql-9.1

# Clean APT cache.
apt-get clean

# Set the password of the user postgres to 'admin'.
su -c "psql -c \"ALTER ROLE postgres WITH PASSWORD 'admin'\"" postgres

# Configure PostgreSQL to listen on all interfaces.
cat <<END >>/etc/postgresql/9.1/main/postgresql.conf

# Listen on all available interfaces:
listen_addresses = '*'
END

# Configure PostgreSQL to allow login from the external interface.
cat <<END >>/etc/postgresql/9.1/main/pg_hba.conf

# Allow external network connections:
host all all 10.0.0.1/8 md5
END

