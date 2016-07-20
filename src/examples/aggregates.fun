'c:/Program Files/Obdobion/funnelsort/examples/data/MyDataVariable.in' 
--columnsIn
  (int --offset  0 --length 1 --name digit) 
  (int --offset 19 --length 7 --name medianHouseholdIncome)
--orderby(digit)
--sum(medianhouseholdincome --name sum1) 
--avg(medianhouseholdincome --name avg1)
--min(medianhouseholdincome --name min1)
--max(medianhouseholdincome --name max1)
--count(--name cnt) 
--formatOut
  (digit)
  (--size 1)(--equation cnt  --length 2  --format '%2d')
  (--size 1)(--equation sum1 --length 10 --format '% ,10d')
  (--size 1)(--equation avg1 --length 10 --format '% ,10d')
  (--size 1)(--equation min1 --length 10 --format '% ,10d')
  (--size 1)(--equation max1 --length 10 --format '% ,10d')
