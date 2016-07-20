'c:/Program Files/Obdobion/funnelsort/examples/data/MyDataVariable.in'
--copy original
--columnsIn
    (-n medianHouseholdIncome --offset 19 --length 7 int)
    (-n stateName --offset 26 --length 21 string)
--formatOut
    (stateName)
    (medianHouseholdIncome)