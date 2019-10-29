#
# Create a SRPM which can be used to build nebula
#
#

%global project_name nebula

Name:     %{project_name}
Version:  %{_version}
Release:  %{_release}%{?dist}
Summary:  %{project_name}
License:  Apache 2.0 + Common Clause 1.0

# BuildRoot dir
BuildRoot:%(mktemp -ud %{_tmppath}/%{name}-%{version}-%{release}-XXXXXX)

# TODO: we should check dependence's version after adapt to different system versions
# BuildRequires only find dynamic libraries but all of nebula dependencies have been compiled to static libraries, so comment out them temporarily
# BuildRequires:   make
# BuildRequires:   autoconf
# BuildRequires:   automake
# BuildRequires:   libtool
# BuildRequires:   unzip
# BuildRequires:   readline
# BuildRequires:   ncurses
# BuildRequires:   ncurses-devel
# BuildRequires:   python
# BuildRequires:   java-1.8.0-openjdk
# BuildRequires:   java-1.8.0-openjdk-devel

%description
A high performance distributed graph database

%prep

%build
cmake -DCMAKE_BUILD_TYPE=Release -DNEBULA_BUILD_VERSION=%{_version} -DCMAKE_INSTALL_PREFIX=%{_install_dir} -DENABLE_TESTING=OFF ./
make -j$(nproc)

%install
rm -rf %{buildroot}
make install DESTDIR=%{buildroot}

%package base
Summary: nebula base package
Group: Applications/Databases
%description base

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

%package console
Summary: nebula console client
Group: Applications/Databases
%description console

%package storage_perf
Summary: perf tool for storage
Group: Applications/Databases
%description storage_perf

%package storage_integrity
Summary: integrity tool for storage
Group: Applications/Databases
%description storage_integrity

%package simple_kv_verify
Summary: kv verify tool
Group: Applications/Databases
%description simple_kv_verify

# the files include exe, config file, scripts
# base rpm include files
%files base
%attr(0755,root,root) %{_datadir}/nebula.service
%attr(0755,root,root) %{_datadir}/utils.sh
%attr(0755,root,root) %{_datadir}/services.sh
%attr(0644,root,root) %{_datadir}/graph.hosts
%attr(0644,root,root) %{_datadir}/meta.hosts
%attr(0644,root,root) %{_datadir}/storage.hosts

# metad rpm include files
%files metad
%attr(0755,root,root) %{_bindir}/nebula-metad
%attr(0644,root,root) %{_sysconfdir}/nebula-metad.conf.default
%attr(0755,root,root) %{_datadir}/nebula-metad.service
%attr(0644,root,root) %{_resourcesdir}/gflags.json

# After install, if config file is non-existent, copy default config file
%post metad
if [[ ! -f %{_install_dir}/etc/nebula-metad.conf ]]; then
    cp %{_install_dir}/etc/nebula-metad.conf.default %{_install_dir}/etc/nebula-metad.conf
fi


# graphd rpm include files
%files graphd
%attr(0755,root,root) %{_bindir}/nebula-graphd
%attr(0644,root,root) %config%{_sysconfdir}/nebula-graphd.conf.default
%attr(0755,root,root) %{_datadir}/nebula-graphd.service
%attr(0644,root,root) %{_resourcesdir}/gflags.json

%post graphd
if [[ ! -f %{_install_dir}/etc/nebula-graphd.conf ]]; then
    cp %{_install_dir}/etc/nebula-graphd.conf.default %{_install_dir}/etc/nebula-graphd.conf
fi


# storaged rpm include files
%files storaged
%attr(0755,root,root) %{_bindir}/nebula-storaged
%attr(0644,root,root) %config%{_sysconfdir}/nebula-storaged.conf.default
%attr(0755,root,root) %{_datadir}/nebula-storaged.service
%attr(0644,root,root) %{_resourcesdir}/gflags.json

%post storaged
if [[ ! -f %{_install_dir}/etc/nebula-storaged.conf ]]; then
    cp %{_install_dir}/etc/nebula-storaged.conf.default %{_install_dir}/etc/nebula-storaged.conf
fi


%files console
%attr(0755,root,root) %{_bindir}/nebula
%attr(0644,root,root) %{_resourcesdir}/completion.json


# storage_perf rpm
%files storage_perf
%attr(0755,root,root) %{_bindir}/storage_perf

# storage_integrity rpm
%files storage_integrity
%attr(0755,root,root) %{_bindir}/storage_integrity

# simple_kv_verify rpm
%files simple_kv_verify
%attr(0755,root,root) %{_bindir}/simple_kv_verify

%debug_package

# missing not found ids
%undefine _missing_build_ids_terminate_build

%changelog
