'c:/Program Files/Obdobion/funnelsort/examples/data/MyLog.in'
--copy original
--columnsIn
  (--name logTime Date --format 'yyyy-MM-dd HH:mm:ss,SSS')
--formatOut
  (--equation 'logTime' --length 13 --format'%1$ta %1$tT')
  (--equation '" orig="' --length 6)
  (logTime)