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

rem Download and install Copssh.
wget -q http://downloads.sourceforge.net/project/sereds/Copssh/4.0.4/Copssh_4.0.4_Installer.zip
7z x Copssh_4.0.4_Installer.zip
Copssh_4.0.4_Installer.exe /S
set PATH=%PATH%;"%programfiles%\ICW\bin"

rem Add an SSH user.
copsshadm --command activateuser --user Administrator
copy identity.pub "%programfiles%\ICW\home\Administrator\.ssh\authorized_keys"

rem Supress the prompt for CD2.
reg add "HKLM\SOFTWARE\Microsoft\Windows\CurrentVersion\R2Setup" /v cd2chain /t REG_DWORD /d 0 /f

rem Shut down.
shutdown /s /t 0 /f

