#!/bin/sh
#
# Create a SRPM which can be used to build nebula
# 
#

%global project_name nebula

Name:     %{project_name}
Version:  @VERSION@
Release:  @RELEASE@%{?dist}
Summary:  %{project_name} 
License:  GPL
# the url to get tar.gz
#URL:      http://
# tar name, this is a temp name
Source:   %{project_name}-@VERSION@.tar.gz

# BuildRoot dir
BuildRoot:%(mktemp -ud %{_tmppath}/%{name}-%{version}-%{release}-XXXXXX)

# TODO: we should check dependence's version after adapt to different system versions
BuildRequires:   gcc, gcc-c++
BuildRequires:   libstdc++-static
BuildRequires:   cmake
BuildRequires:   make
BuildRequires:   autoconf
BuildRequires:   automake
BuildRequires:   flex
BuildRequires:   gperf
BuildRequires:   libtool
BuildRequires:   bison
BuildRequires:   unzip
BuildRequires:   boost
BuildRequires:   boost-devel
BuildRequires:   boost-static
BuildRequires:   openssl
BuildRequires:   openssl-devel
BuildRequires:   libunwind
BuildRequires:   libunwind-devel
BuildRequires:   ncurses
BuildRequires:   ncurses-devel
BuildRequires:   readline
BuildRequires:   readline-devel
BuildRequires:   python
BuildRequires:   java-1.8.0-openjdk
BuildRequires:   java-1.8.0-openjdk-devel
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
%{_bindir}/nebula-metad
%{_datadir}/nebula-metad.service

# TODO : add daemon to systemctl

# after install , arg 1:install new packet, arg 2: update exist packet
#%%post metad
#%%systemd_post nebula-metad.service

# before uninstall, arg 0:delete  arg 1:update
#%%preun metad
#%%systemd_preun nebula-metad.service

# upgrade, arg 0:delete  arg 1:update
#%%postun metad
#%%systemd_postun nebula-metad.service

# graphd rpm
%files graphd
%defattr(-,root,root,-)
%{_bindir}/nebula-graphd
%config(noreplace) %{_sysconfdir}/nebula-graphd.conf.default
%{_datadir}/nebula-graphd.service

# TODO : add daemon to systemctl

#%%post graphd
#%%systemd_post nebula-graphd.service

#%%preun graphd
#%%systemd_preun nebula-graphd.service

#%%postun graphd
#%%systemd_postun nebula-graphd.service

%files storaged
%defattr(-,root,root,-)
%{_bindir}/nebula-storaged
%{_datadir}/nebula-storaged.service

#%%post storaged
#%%systemd_post nebula-storaged.service

#%%preun storaged
#%%systemd_preun nebula-storaged.service

#%%postun storaged
#%%systemd_postun nebula-storaged.service

%files nebula
%defattr(-,root,root,-)
%{_bindir}/nebula


# storage_perf rpm
#%%files storage_perf
#%%defattr(-,root,root,-)
#%%{_bindir}/storage_perf

%changelog

