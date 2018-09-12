@echo off
call :treeProcess
goto :eof

:treeProcess
for %%f in ("*.blwp") do echo %%f
for /D %%d in ("C:\botw-data\decompressed\content\Map\MainField\*") do (
    echo %%d
    cd %%d
    call :treeProcess
    cd ..
)
exit /b