%{!?python_sitelib: %global python_sitelib %(%{__python} -c "from distutils.sysconfig import get_python_lib; print get_python_lib()")}

Name:           ${NAME}
Version:        ${VERSION}
Release:        1%{?dist}
Summary:        ${SUMMARY}

Group:          Application/Databases
License:        AGPLv3 with 7b attribution and GPLv2 exception
URL:            http://htsql.org/
Source:         ${PACKAGE}.tar.gz
BuildRoot:      %{_tmppath}/%{name}-%{version}-%{release}-root-%(%{__id_u} -n)
Prefix:         %{_prefix}

BuildArch:      noarch
Vendor:         Prometheus Research, LLC
BuildRequires:  python-devel, python-setuptools
Requires:       ${REQUIRES}

%description
HTSQL is a comprehensive navigational query language for relational
databases.  HTSQL is designed for data analysts and other accidental
programmers who have complex business inquiries to solve and need a
productive tool to write and share database queries.  HTSQL is free
and open source software.  For more detail, visit http://htsql.org/.

${DESCRIPTION}

%prep
%setup -q -n ${PACKAGE}

%build
%{__python} setup.py build

%install
rm -rf $RPM_BUILD_ROOT
%{__python} setup.py install -O1 --skip-build --root $RPM_BUILD_ROOT
 
%clean
rm -rf $RPM_BUILD_ROOT

%files
%defattr(-,root,root,-)
%doc ${DOC_FILES}
${FILES}
# For noarch packages: sitelib
%{python_sitelib}/*

%changelog
* Thu Aug 23 2012 Kirill Simonov <xi@resolvent.net> - 2.3.2-1
- Upstream release
* Thu Jun 28 2012 Kirill Simonov <xi@resolvent.net> - 2.3.1-1
- Upstream release
* Tue Mar 06 2012 Clark C. Evans <cce@clarkevans.com> - 2.3.0-1
- Upstream release
* Thu Dec 15 2011 Clark C. Evans <cce@clarkevans.com> - 2.2.1-1
- Upstream release
* Tue Nov 22 2011 Clark C. Evans <cce@clarkevans.com> - 2.2.0c1-1
- Upstream release
* Fri Nov 11 2011 Clark C. Evans <cce@clarkevans.com> - 2.2.0b2-1
- Initial packaging 
