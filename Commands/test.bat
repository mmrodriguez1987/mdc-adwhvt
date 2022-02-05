@echo off
ECHO "Data Validation Init"

for /f "skip=1" %%x in ('wmic os get localdatetime') do if not defined MyDate set MyDate=%%x
for /f %%x in ('wmic path win32_localtime get /format:list ^| findstr "="') do set %%x
set fmonth=00%Month%
set fday=00%Day%
set today=%Year%-%fmonth:~-2%-%fday:~-2%
echo %today%

set pServer=http://s0146154:443

SET URL1=%pServer%/api/AccountTest?startDate=%today%endDate=%today%

echo %URL1%



set list=Accounttest BillSegmentTest PersonTest PremiseTest BillUsageTest 
(for %%b in (%list%) do ( 
   echo %%b 

    SET URL7=%pServer%/api/%%b?startDate=%today%endDate=%today%
    echo %URL7%
    SET HTTP7=
    for /f %%a in ( 'curl -s -o nul -w  "%%{http_code}" "%URL7%"' ) do set HTTP7=%%a

    if "%HTTP7%" == "200" (
        echo %%b" ok"   
    ) else (
        echo %%b" fail"        
    )

))