#!/bin/sh

#
# Create a SRPM which can be used to build nebula
#
# ./make-srpm.sh <rpmbuild dir>
# 
#

%global project_name nebula

Name:     %{project_name}
Version:  1.0.0
Release:  1%{?dist}
Summary:  %{project_name} 
License:  GPL

# package tar name, this is a temp name
Source:   nebula-1.0.0.tar.gz

# BuildRoot dir
BuildRoot:%(mktemp -ud %{_tmppath}/%{name}-%{version}-%{release}-XXXXXX)

BuildRequires:   gcc, gcc-c++
BuildRequires:   cmake
BuildRequires:   autoconf
BuildRequires:   automake
BuildRequires:   libtool
BuildRequires:   bison
BuildRequires:   unzip
BuildRequires:   boost
BuildRequires:   gperf
BuildRequires:   openssl
BuildRequires:   libunwind
BuildRequires:   ncurses
BuildRequires:   readline
Requires:        krb5

%description 
A high performance distributed graph database

%prep
%setup -q

%build
cmake ./
make -j

%install
rm -rf %{buildroot}
make install DESTDIR=%{buildroot}

%package metad
Summary: nebula meta server daemon
Group: Applications/Databases
%description metad
metad is a daemon for manage metadata

%package graphd
Summary: graph daemon
Group: Applications/Databases
%description graphd 
graphd is a daemon for handle graph data

%package storaged
Summary: storaged daemon
Group: Applications/Databases
%description storaged 
storaged is a daemon for storage all data

%package nebula
Summary: nebula console client
Group: Applications/Databases
%description nebula

%package storage_perf
Summary: tool for storage
Group: Applications/Databases
%description storage_perf


#################################################################################
# the files include exe, config file, scriptlets
#################################################################################
# metad rpm 
%files metad
%defattr(-,root,root,-)
%{_bindir}/metad
%{_datadir}/nebula-metad.service

# TODO : add daemon to systemctl

# after install , arg 1:install new packet, arg 2: update exist packet
%post metad 
%systemd_post nebula-metad.service

# before uninstall, arg 0:delete  arg 1:update
%preun metad  
%systemd_preun nebula-metad.service

# upgrade, arg 0:delete  arg 1:update
%postun metad 
%systemd_postun nebula-metad.service

# graphd rpm
%files graphd
%defattr(-,root,root,-)
%{_bindir}/nebula-graphd
%config(noreplace) %{_sysconfdir}/nebula-graphd.conf.default
%{_datadir}/nebula-graphd.service

# TODO : add daemon to systemctl

%post graphd
%systemd_post nebula-graphd.service  

%preun graphd
%systemd_preun nebula-graphd.service

%postun graphd
%systemd_postun nebula-graphd.service 

%files storaged
%defattr(-,root,root,-)
%{_bindir}/storaged
%{_datadir}/nebula-storaged.service

%post storaged
%systemd_post nebula-storaged.service 

%preun storaged
%systemd_preun nebula-storaged.service

%postun storaged
%systemd_postun nebula-storaged.service

%files nebula
%defattr(-,root,root,-)
%{_bindir}/nebula


# storage_perf rpm
#%%files storage_perf
#%%defattr(-,root,root,-)
#%%{_bindir}/storage_perf

%changelog

