@echo off
cd %systemdrive%\INSTALL

rem Post-installation script for MS Windows.

rem Install Wget.
wget-1.11.4-1-setup.exe /silent

set PATH=%PATH%;"%programfiles%\GnuWin32\bin"

rem Download and install 7-Zip.
wget -q http://downloads.sourceforge.net/sevenzip/7z465.exe
7z465.exe /S
set PATH=%PATH%;"%programfiles%\7-Zip"

rem Download and install Cygwin.
wget -q http://cygwin.com/setup-x86.exe
setup-x86.exe -q -s http://mirrors.kernel.org/sourceware/cygwin -R C:\cygwin -l C:\cygwin\download -P openssh
set PATH=%PATH%;"C:\cygwin\bin"
bash -l -c ""

rem Configure SSH.
bash -c "ssh-host-config -y -w cyg_server"
bash -c "mkdir /home/Administrator/.ssh"
bash -c "chmod go-rwx /home/Administrator/.ssh"
bash -c "cp identity.pub /home/Administrator/.ssh/authorized_keys"

rem Supress the prompt for CD2.
reg add "HKLM\SOFTWARE\Microsoft\Windows\CurrentVersion\R2Setup" /v cd2chain /t REG_DWORD /d 0 /f

rem Cleanup.
del /q wget-1.11.4-1-setup.exe
del /q 7z465.exe
del /q setup.exe
del /q identity.pub

rem Shut down.
shutdown /s /t 0 /f

