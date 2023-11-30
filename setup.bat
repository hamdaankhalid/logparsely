@echo off

set "rustupVersion=1.25.1"
set "$sqliteVersion=3.12.2"

choco -V >> NUL
IF %ErrorLevel% NEQ 0 @"%SystemRoot%\System32\WindowsPowerShell\v1.0\powershell.exe" -NoProfile -InputFormat None -ExecutionPolicy Bypass -Command " [System.Net.ServicePointManager]::SecurityProtocol = 3072; iex ((New-Object System.Net.WebClient).DownloadString('https://chocolatey.org/install.ps1'))" && SET "PATH=%PATH%;%ALLUSERSPROFILE%\chocolatey\bin"

rustup --help >> NUL
IF %ErrorLevel% NEQ 0 choco install rustup.install -y --version %rustupVersion%

set "url=https://download.sqlitebrowser.org/SQLiteDatabaseBrowserPortable_3.12.2_English.paf.exe"
set "outputFile=SQLiteDatabaseBrowserPortable_3.12.2_English.paf.exe"
set "installerPath=%outputFile%"

curl -O %url%

if %errorlevel% neq 0 (
    echo Error: Failed to download the sqlitebrowser installer file.
    exit /b 1
)

echo Download completed successfully.

echo Running the installer...
start "" "%installerPath%"

refreshenv
