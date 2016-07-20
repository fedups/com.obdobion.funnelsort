'c:/Program Files/Obdobion/funnelsort/examples/data/MyLog.in'
--columnsIn(--name message String --offset 34)
--where 'not(empty(matches(message, "BEGIN")))'
--count(-n jobCount)
--formatOut(-e jobCount -l 2)(-s1)(-e'"jobs"' -l 4)