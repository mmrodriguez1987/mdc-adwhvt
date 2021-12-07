@echo off
SETLOCAL
ECHO %DATE% %TIME% Calling %0 with %*

SET URL=http://s0146154/api/GlobalTest/

SET HTTP=
for /f %%a in ( 'curl -s -o nul -w  "%%{http_code}" "%URL%"' ) do set HTTP=%%a

if "%HTTP%" == "200" (
    echo "ok"
    exit /b 0
) else (
    echo "fail"
    exit /b 1
)
