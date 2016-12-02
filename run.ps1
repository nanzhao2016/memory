param (
	[switch]$help,
	[switch]$h,
	[switch]$info,
	[switch]$i,
	[switch]$step,
	[switch]$s,
	[int]$var=0
)	

if($help -or $h) {
	Write-Host "help: show basic information help/h"
	Write-Host "info: show available processes information, info/i"
	Write-Host "step: choose a process step/s, with a process argument between 1 and 3"
}
elseif($info -or $i){
	Write-Host "Available process: "
	Write-Host "1. Save original data to parquet"
	Write-Host "2. Update the format for the original data"
	Write-Host "3. Basical descriptive analysis"
}
elseif($step -or $s){
	spark-submit C:\Users\OPEN\Documents\NanZHAO\Formation_BigData\Memoires\draft.py $var
}
else{
	Write-Host "Please choose one of the following options: "
	Write-Host "-help"
	Write-Host "-info"
	Write-Host "-step"
}


