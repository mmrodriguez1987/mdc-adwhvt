@echo on
ECHO "Validaton Test on Staging Enviroment"

set pServer=http://s0146154:443

Rem This is for find the startDate and endDate in the rquired format
for /f "skip=1" %%x in ('wmic os get localdatetime') do if not defined MyDate set MyDate=%%x
for /f %%x in ('wmic path win32_localtime get /format:list ^| findstr "="') do set %%x
set fmonth=00%Month%
set fday=00%Day%
set today=%Year%-%fmonth:~-2%-%fday:~-2%
echo %today%

SET URL1=%pServer%/api/AccountTest?startDate=%today%endDate=%today%
SET HTTP1=

for /f %%a in ( 'curl -s -o nul -w  "%%{http_code}" "%URL1%"' ) do set HTTP1=%%a

if "%HTTP1%" == "200" (
    echo "Account Test ok"   
) else (
    echo "Account Test fail"
    exit /b 1
)

GOTO comment

set Testlist=AccountTest BillSegmentTest PersonTest PremiseTest BillUsageTest ServiceAgreementTest
(for %%b in (%list%) do ( 
    echo %%b 
    SET URL=%pServer%/api/%%b?startDate=%today%endDate=%today%
    echo %URL%
    SET HTTP=
    for /f %%a in ( 'curl -s -o nul -w  "%%{http_code}" "%URL%"' ) do set HTTP=%%a
    if "%HTTP%" == "200" (
        echo %%b" ok"   
    ) else (
        echo %%b" fail"        
    )
))

:comment

echo "Test finisihed"