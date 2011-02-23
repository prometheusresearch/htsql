@echo off
cd %systemdrive%\INSTALL

rem Post-installation script for the mssql2008 VM.

rem For instructions on installing MS SQL Server 2008 R2, see
rem http://msdn.microsoft.com/en-us/library/ms143219.aspx

rem SQL Server 2005 requires Service Pack 1 when installing on
rem Windows Server 2003, or Service Pack 2 when installing on
rem Windows XP.  The other prerequisite is .NET Framework 2.0.

rem Prerequisites: .NET Framework 2.0 SP2, Windows Installer 4.5

set PATH=%PATH%;"%programfiles%\GnuWin32\bin"
set PATH=%PATH%;"%programfiles%\7-Zip"

rem .NET Framework 2.0 SP2
wget -q http://download.microsoft.com/download/c/6/e/c6e88215-0178-4c6c-b5f3-158ff77b1f38/NetFx20SP2_x86.exe
7z x NetFx20SP2_x86.exe -oNetFx20SP2_x86
cd NetFx20SP2_x86
setup.exe /q
cd ..

rem Windows Installer 4.5
wget -q http://download.microsoft.com/download/2/6/1/261fca42-22c0-4f91-9451-0e0f2e08356d/WindowsServer2003-KB942288-v4-x86.exe
WindowsServer2003-KB942288-v4-x86.exe /quiet /norestart

rem MS SQL Server 2008 R2 Express
wget -q http://download.microsoft.com/download/5/1/A/51A153F6-6B08-4F94-A7B2-BA1AD482BC75/SQLEXPR32_x86_ENU.exe
SQLEXPR32_x86_ENU.exe /Q /IACCEPTSQLSERVERLICENSETERMS /ACTION=install /FEATURES=SQL /INSTANCENAME=MSSQLSERVER /SECURITYMODE=SQL /SAPWD=tot8OmDeufi /SQLSVCACCOUNT="NT AUTHORITY\NETWORK SERVICE" /TCPENABLED=1

set path=%path%;"%programfiles%\Microsoft SQL Server\100\Tools\binn"
sqlcmd -Q "ALTER LOGIN sa WITH PASSWORD='admin', CHECK_POLICY=OFF"

rem Shut down.
shutdown /s /t 0 /f


