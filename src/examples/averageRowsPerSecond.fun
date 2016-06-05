'c:/Program Files/funnel/examples/data/MyLog.in'
--columnsIn(--name message String --offset 34)
--where 'not(empty(matches(message, "rowsPerSecond")))'
--avg(-n avgRPS -e 'toInt(matches(message, "rowsPerSecond\\(([0-9]+)\\)"))')
--formatOut(-e avgRPS -l 6)(-s1)(-e'"average rows per second"' -l 23)