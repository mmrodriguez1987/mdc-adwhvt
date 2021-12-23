@echo on
ECHO "Data Validation Init"

SET URL1=http://s0146154:443/api/AccountTest/

SET HTTP1=
for /f %%a in ( 'curl -s -o nul -w  "%%{http_code}" "%URL1%"' ) do set HTTP1=%%a

if "%HTTP1%" == "200" (
    echo "Account Test ok"   
) else (
    echo "Account Test fail"
    exit /b 1
)


SET URL3=http://s0146154:443/api/BillUsageTest/

SET HTTP3=
for /f %%a in ( 'curl -s -o nul -w  "%%{http_code}" "%URL3%"' ) do set HTTP3=%%a

if "%HTTP3%" == "200" (
    echo "Bill Usage Test ok"   
) else (
    echo "Bill Usage Test fail"
    exit /b 1
)

SET URL4=http://s0146154:443/api/PersonTest/

SET HTTP4=
for /f %%a in ( 'curl -s -o nul -w  "%%{http_code}" "%URL4%"' ) do set HTTP4=%%a

if "%HTTP4%" == "200" (
    echo "Person Test ok"   
) else (
    echo "Person Test fail"
    exit /b 1
)

SET URL5=http://s0146154:443/api/PremiseTest/

SET HTTP5=
for /f %%a in ( 'curl -s -o nul -w  "%%{http_code}" "%URL5%"' ) do set HTTP5=%%a

if "%HTTP5%" == "200" (
    echo "Premise Test ok"  
) else (
    echo "Premise Test fail"
    exit /b 1
)

SET URL6=http://s0146154:443/api/ServiceAgreementTest/

SET HTTP6=
for /f %%a in ( 'curl -s -o nul -w  "%%{http_code}" "%URL6%"' ) do set HTTP6=%%a

if "%HTTP6%" == "200" (
    echo "Service Agreement Test ok"   
) else (
    echo "Service Agreement Test fail"
    exit /b 1
)


SET URL2=http://s0146154:443/api/BillSegmentTest/

SET HTTP2=
for /f %%a in ( 'curl -s -o nul -w  "%%{http_code}" "%URL2%"' ) do set HTTP2=%%a

if "%HTTP2%" == "200" (
    echo "Bill Segment Test ok"   
) else (
    echo "Bill Segment Test fail"
    exit /b 1
)


echo "Test finisihed"