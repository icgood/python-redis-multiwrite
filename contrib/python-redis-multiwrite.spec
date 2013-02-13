Name:           python-redis-multiwrite
Version:        20121025
Release:        3%{?dist}
Summary:        Execute redis commands against multiple servers simultaneously.

License:        ASL 2.0
URL:            https://github.rackspace.com/sao-paulo/python-redis-multiwrite
Source:		python-redis-multiwrite-%{version}.tar.bz2

BuildArch:	noarch
BuildRequires:  python, python-setuptools
Requires:	python-redis, python-eventlet

%description
Execute redis commands against multiple servers simultaneously.

%prep

%setup -q -n python-redis-multiwrite

%build
%{__python} setup.py build

%install
%{__python} setup.py install --skip-build --root %{buildroot}

%pre

%files
%defattr(-,root,root,-)
%{python_sitelib}/redismultiwrite.py*
%{python_sitelib}/redis_multiwrite*.egg-info

%changelog
* Mon Dec 03 2012 Ian Good <ian.good@rackspace.com> 20121203-1
- Created specfile
