%{!?python_sitelib: %global python_sitelib %(%{__python} -c "from distutils.sysconfig import get_python_lib; print get_python_lib()")}

Name:           HTSQL
Version:        2.2.0c1
Release:        1%{?dist}
Summary:        navigational query language for relational databases

Group:          Application/Databases
License:        Free To Use But Restricted
URL:            http://htsql.org 
Source:         %{name}-%{version}.tar.gz
BuildRoot:      %{_tmppath}/%{name}-%{version}-%{release}-root-%(%{__id_u} -n)
Prefix:         %{_prefix}

BuildArch:      noarch
Vendor:         Prometheus Research, LLC
BuildRequires:  python-devel, python-setuptools
Requires:       PyYAML, python-setuptools

%description
***********************************************************
  HTSQL -- A Query Language for the Accidental Programmer
***********************************************************

HTSQL ("Hyper Text Structured Query Language") is a high-level query
language for relational databases.   The target audience for HTSQL is
the accidental programmer -- one who is not a SQL expert, yet needs a
usable, comprehensive query tool for data access and reporting.  

HTSQL is also a web service which takes a request via HTTP, translates
it into a SQL query, executes the query against a relational database,
and returns the results in a format requested by the user agent (JSON,
CSV, HTML, etc.).

Use of HTSQL with open source databases (PostgreSQL, MySQL, SQLite) is
royalty free under BSD-style conditions.  Use of HTSQL with proprietary
database systems (Oracle, Microsoft SQL) requires a commercial license.
See ``LICENSE`` for details.

For installation instructions, see ``INSTALL``.  For list of new
features in this release, see ``NEWS``.  HTSQL documentation is in the
``doc`` directory. 

    http://htsql.org/
        The HTSQL homepage

    http://htsql.org/doc/introduction.html
        Get taste of HTSQL

    http://htsql.org/doc/tutorial.html
        The HTSQL tutorial

    http://bitbucket.org/prometheus/htsql
        HTSQL source code

    irc://irc.freenode.net#htsql
        IRC chat in #htsql on freenode

    http://lists.htsql.org/mailman/listinfo/htsql-users
        The mailing list for users of HTSQL

HTSQL is copyright by Prometheus Research, LLC.  HTSQL is written by
Clark C. Evans <cce@clarkevans.com> and Kirill Simonov <xi@resolvent.net>.

Generous support for HTSQL was provided by the Simons Foundation.
This material is also based upon work supported by the National
Science Foundation under Grant #0944460.


%prep
%setup -q -n %{name}-%{version}

%build
%{__python} setup.py build

%install
rm -rf $RPM_BUILD_ROOT
%{__python} setup.py install -O1 --skip-build --root $RPM_BUILD_ROOT
 
%clean
rm -rf $RPM_BUILD_ROOT

%files
%defattr(-,root,root,-)
%doc README NEWS LICENSE AUTHORS doc
/usr/bin/htsql-ctl
# For noarch packages: sitelib
%{python_sitelib}/*

%changelog
* Tue Nov 22 2011 Clark C. Evans <cce@clarkevans.com> - 2.2.0c1-1
- Upstream release
* Fri Nov 11 2011 Clark C. Evans <cce@clarkevans.com> - 2.2.0b2-1
- Initial packaging 
