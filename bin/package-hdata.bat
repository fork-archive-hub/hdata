@echo on
cd /d %~dp0..
set HDATA_HOME=%cd%
set HDATA_BIN_DIR=%HDATA_HOME%\bin
set HDATA_CONF_DIR=%HDATA_HOME%\conf
set HDATA_BUILD_DIR=%HDATA_HOME%\build
set HDATA_BUILD_HDATA_DIR=%HDATA_HOME%\build\hdata
:: mvn not to exit command
set MAVEN_TERMINATE_CMD=off

::if exist "%HDATA_BUILD_DIR%" del /F /S /Q "%HDATA_BUILD_DIR%"
if exist "%HDATA_BUILD_DIR%" rm /S /Q "%HDATA_BUILD_DIR%"

mkdir "%HDATA_BUILD_HDATA_DIR%\lib"
mkdir "%HDATA_BUILD_HDATA_DIR%\bin"
mkdir "%HDATA_BUILD_HDATA_DIR%\plugins"
mkdir "%HDATA_BUILD_HDATA_DIR%\samples"

::copy hdata's bash script to bin path
copy /y "%HDATA_BIN_DIR%\hdata" "%HDATA_BUILD_HDATA_DIR%\bin\"
copy /y "%HDATA_BIN_DIR%\hdata.bat" "%HDATA_BUILD_HDATA_DIR%\bin\"
:: copy conf path to %HDATA_BUILD_HDATA_DIR%\conf
echo D| xcopy "%HDATA_HOME%\conf" "%HDATA_BUILD_HDATA_DIR%\conf" /S /E
:: copy samples to %HDATA_BUILD_HDATA_DIR%\samples
if exist "%HDATA_HOME%\samples" echo D| xcopy "%HDATA_HOME%\samples" "%HDATA_BUILD_HDATA_DIR%\samples" /S /E
::copy readme
copy "%HDATA_HOME%\readme.md" "%HDATA_BUILD_HDATA_DIR%"



call mvn clean package dependency:copy-dependencies 

copy /y %HDATA_HOME%\hdata-core\target\hdata-core-*.jar %HDATA_BUILD_HDATA_DIR%\lib
copy /y %HDATA_HOME%\hdata-core\target\dependency\*.jar %HDATA_BUILD_HDATA_DIR%\lib
copy /y %HDATA_HOME%\hdata-api\target\dependency\*.jar %HDATA_BUILD_HDATA_DIR%\lib

setlocal ENABLEDELAYEDEXPANSION
for /f %%i in ('dir /b /ad') do (
    ( echo %%i|find /i "hdata-">nul ) && (
       (echo %%i|find /i "hdata-core"> nul ) || ( echo %%i|find /i "hdata-api"> nul ) || (
	   set d=%%i
	   set d=!d:~6,100!
       set pluginDir=%HDATA_BUILD_HDATA_DIR%\plugins\!d!
       if not exist "!pluginDir!" mkdir "!pluginDir!"
	   if exist ".\%%i\target\dependency" copy /y ".\%%i\target\dependency\*.jar" "!pluginDir!" 
      )
  )
)

set path=%HDATA_BIN_DIR%;%path%
if exist "%HDATA_BUILD_DIR%" (
cd /d "%HDATA_BUILD_DIR%"
if exist "%HDATA_BUILD_DIR%\hdata.zip" del /q "%HDATA_BUILD_DIR%\hdata.zip"
zip -r hdata.zip hdata
)
