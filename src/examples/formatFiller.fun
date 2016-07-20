'c:/Program Files/Obdobion/funnelsort/examples/data/MyDataVariable.in'
--copy original
--columnsIn
    (-n medianHouseholdIncome --offset 19 --length 7 int)
    (-n stateName --offset 26 --length 21 string)
--formatOut
    (stateName --filler 95)
    (--size 1)
    (--size 4 --filler *)
    (--size 1)
    (medianHouseholdIncome)