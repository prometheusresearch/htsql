@echo off
cd %systemdrive%\INSTALL

rem Post-installation script for the mssql2005 VM.

rem For instructions on installing MS SQL Server 2005 Express Edition, see
rem http://www.microsoft.com/downloads/details.aspx?familyid=220549b5-0b07-4448-8848-dcc397514b41&displaylang=en
rem SQL Server 2005 requires Service Pack 1 when installing on
rem Windows Server 2003, or Service Pack 2 when installing on
rem Windows XP.  The other prerequisite is .NET Framework 2.0.

set PATH=%PATH%;"%programfiles%\GnuWin32\bin"
set PATH=%PATH%;"%programfiles%\7-Zip"


rem Supress the prompt for CD2.
reg add "HKLM\SOFTWARE\Microsoft\Windows\CurrentVersion\R2Setup" /v cd2chain /t REG_DWORD /d 0 /f


rem Download and install .NET Framework 2.0.
wget -q http://download.microsoft.com/download/5/6/7/567758a3-759e-473e-bf8f-52154438565a/dotnetfx.exe
rem dotnetfx /C /T:%systemdrive%/INSTALL/dotnetfx
7z x dotnetfx.exe -odotnetfx
cd dotnetfx
install /q
cd ..

rem Download and install SQL Server 2005 Express Edition.
wget -q http://download.microsoft.com/download/f/1/0/f10c4f60-630e-4153-bd53-c3010e4c513b/SQLEXPR.EXE
rem SQLEXPR.EXE /x:%systemdrive%/INSTALL/SQLEXPR
7z x SQLEXPR.EXE -oSQLEXPR
cd SQLEXPR
setup /qn ADDLOCAL=ALL SECURITYMODE=SQL SAPWD=tot8OmDeufi DISABLENETWORKPROTOCOLS=0
cd ..

rem Change the password for the SQL Server administrator account.
set PATH=%PATH%;"%programfiles%\Microsoft SQL Server\90\Tools\binn"
sqlcmd -S \SQLEXPRESS -Q "ALTER LOGIN sa WITH PASSWORD='admin', CHECK_POLICY=OFF"

rem Enable TCP access to the SQL Server.
reg add "HKLM\SOFTWARE\Microsoft\Microsoft SQL Server\MSSQL.1\MSSQLServer\SuperSocketNetLib\Tcp\IP1" /v Enabled /t REG_DWORD /d 1 /f
reg add "HKLM\SOFTWARE\Microsoft\Microsoft SQL Server\MSSQL.1\MSSQLServer\SuperSocketNetLib\Tcp\IP1" /v TcpPort /t REG_SZ /d 1433 /f
reg add "HKLM\SOFTWARE\Microsoft\Microsoft SQL Server\MSSQL.1\MSSQLServer\SuperSocketNetLib\Tcp\IP2" /v Enabled /t REG_DWORD /d 1 /f
reg add "HKLM\SOFTWARE\Microsoft\Microsoft SQL Server\MSSQL.1\MSSQLServer\SuperSocketNetLib\Tcp\IP2" /v TcpPort /t REG_SZ /d 1433 /f
reg add "HKLM\SOFTWARE\Microsoft\Microsoft SQL Server\MSSQL.1\MSSQLServer\SuperSocketNetLib\Tcp\IPAll" /v TcpPort /t REG_SZ /d 1433 /f

rem Shut down.
shutdown /s /t 0 /f

