@echo off
cd /d %~dp0..
setlocal ENABLEDELAYEDEXPANSION
set HDATA_HOME=%cd%
set HDATA_LIB_DIR=%HDATA_HOME%\lib
set HDATA_CONF_DIR=%HDATA_HOME%\conf
set HDATA_PLUGINS_DIR=%HDATA_HOME%\plugins

if not defined java_home ( 
echo "Not defined JAVA_HOME,Please install java in your PATH and set JAVA_HOME"
call :timeoutAndExit
) 
set JAVA="%JAVA_HOME%\bin\java.exe"

if not exist %JAVA% (
    echo "Could not find any executable java binary. Please install java in your PATH or set JAVA_HOME"
    call :timeoutAndExit
)

set HDATA_CLASSPATH=.;%HDATA_LIB_DIR%\*
::add plugins to class_path
::for /f %%i in ('dir /b /ad %HDATA_PLUGINS_DIR%') do (
::set HDATA_CLASSPATH=!HDATA_CLASSPATH!;!HDATA_PLUGINS_DIR!\%%i\*
::)
echo %HDATA_CLASSPATH% 

set JAVA_OPTS=%JAVA_OPTS% -Xss256k
set JAVA_OPTS=%JAVA_OPTS% -Xms1G -Xmx1G -Xmn512M
set JAVA_OPTS=%JAVA_OPTS% -XX:+UseParNewGC
set JAVA_OPTS=%JAVA_OPTS% -XX:+UseConcMarkSweepGC
set JAVA_OPTS=%JAVA_OPTS% -XX:+CMSClassUnloadingEnabled
set JAVA_OPTS=%JAVA_OPTS% -XX:+CMSParallelRemarkEnabled
set JAVA_OPTS=%JAVA_OPTS% -XX:+DisableExplicitGC
set JAVA_OPTS=%JAVA_OPTS% -XX:CMSInitiatingOccupancyFraction=75
set JAVA_OPTS=%JAVA_OPTS% -XX:+UseCMSInitiatingOccupancyOnly
set JAVA_OPTS=%JAVA_OPTS% -XX:+HeapDumpOnOutOfMemoryError
set JAVA_OPTS=%JAVA_OPTS% -XX:SoftRefLRUPolicyMSPerMB=0

set JAVA_OPTS=%JAVA_OPTS% -Dhdata.conf.dir="%HDATA_CONF_DIR%"
set JAVA_OPTS=%JAVA_OPTS% -Dlog4j.configurationFile=file:///%HDATA_CONF_DIR%\log4j2.xml

set MAIN_CLASS="com.github.stuxuhai.hdata.CliDriver"

echo %JAVA% %JAVA_OPTS% -classpath "%HDATA_CLASSPATH%" %MAIN_CLASS% %1 %2 %3 %4 %5 %6 %7 %8 %9
%JAVA% %JAVA_OPTS% -classpath "%HDATA_CLASSPATH%" %MAIN_CLASS% %1 %2 %3 %4 %5 %6 %7 %8 %9

goto :EOF

:timeoutAndExit
timeout /t 10&&exit
