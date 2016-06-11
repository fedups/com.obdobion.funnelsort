'c:/Program Files/funnel/examples/data/MyDataWithHeader.in'  
 --columnsIn(--name medianHouseholdIncome --offset 19 --length 7 int) 
 --orderBy(medianHouseholdIncome)
 --headerIn
  (date   -l8 -n RUNDATE -d'yyyyMMdd')
  (filler -l1)
  (string -l4 -n RUNTYPE)