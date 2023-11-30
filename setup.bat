@echo off

set "rustupVersion=1.25.1"
set "$sqliteVersion=3.12.2"

choco -V >> NUL
IF %ErrorLevel% NEQ 0 @"%SystemRoot%\System32\WindowsPowerShell\v1.0\powershell.exe" -NoProfile -InputFormat None -ExecutionPolicy Bypass -Command " [System.Net.ServicePointManager]::SecurityProtocol = 3072; iex ((New-Object System.Net.WebClient).DownloadString('https://chocolatey.org/install.ps1'))" && SET "PATH=%PATH%;%ALLUSERSPROFILE%\chocolatey\bin"

rustup --help >> NUL
IF %ErrorLevel% NEQ 0 choco install rustup.install -y --version %rustupVersion%

sqlite3 -version >> NULL
IF %ErrorLevel% NEQ 0 choco install sqlitebrowser -y --version %sqliteVersion%

refreshenv
