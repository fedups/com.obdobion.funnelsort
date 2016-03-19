%define name      funnel
%define version   @ANT.version@
%define release   1
%define _tmppath  /tmp
%define buildroot %{_topdir}/BUILD/%{name}-%{version}
%define _prefix   /usr/local

Name: %{name}
Version: %{version}
Release: %{release}
Source: %{name}-%{version}-app.tar.bz2

Summary: Funnel Sort Utility
License: GPL
Group: System/Tools 
URL: http://www.littlegraycloud/funnel
Distribution: Linux
Vendor: Obdobion Corporation
Packager: Chris DeGreef <chris.degreef@gmail.com>
Prefix:    %{_prefix}
Buildroot: %{buildroot}

%description
Funnel is a sort utility to sort files, large and small.

%prep
%setup -q

%install
cp -R * %{buildroot}

mkdir -p %{buildroot}/etc
ln -s -f /opt/funnel/etc             %{buildroot}/etc/funnel

mkdir -p %{buildroot}/var/log
ln -s -f /opt/funnel/var/log    %{buildroot}/var/log/funnel

mkdir -p %{buildroot}/usr/bin
ln -s -f /opt/funnel/bin/funnel   %{buildroot}/usr/bin/funnel

mkdir -p %{buildroot}/etc/profile.d
ln -s -f /opt/funnel/etc/profile.d/funnel.sh  %{buildroot}/etc/profile.d

mkdir -p %{buildroot}/etc/logrotate.d
ln -s -f /opt/funnel/etc/logrotate.d/funnel  %{buildroot}/etc/logrotate.d

#%post


%files
%defattr(644,root,root)

#links
%attr(711,root,root) /usr/bin/funnel
%attr(444,root,root) /etc/funnel
%attr(700,root,root) /etc/profile.d/funnel.sh
%attr(700,root,root) /etc/logrotate.d/funnel

# The one and only script to run funnel

%attr(755,root,root) /opt/funnel/bin/funnel
%attr(710,root,root) /opt/funnel/etc/profile.d/funnel.sh
%attr(710,root,root) /opt/funnel/etc/logrotate.d/funnel

# jar files

/opt/funnel/lib/funnel-%{version}.jar
/opt/funnel/lib/argument-2.2.37.jar
/opt/funnel/lib/algebrain-1.2.4.jar
/opt/funnel/lib/log4j-1.2.17.jar

# System configuration file for funnel.  This is referenced from the bin script.

%config(noreplace) %attr(644,root,root) /opt/funnel/etc/conf
%config(noreplace) %attr(644,root,root) /opt/funnel/etc/log4j.xml

# convenience links
%attr(440,root,root) /etc/funnel
%attr(710,root,root) /var/log/funnel

# All provided .fun files in the /opt/funnel/etc/funnel directory

/opt/funnel/etc/dos2unix
/opt/funnel/etc/unix2dos
