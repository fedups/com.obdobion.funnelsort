'c:/Program Files/Obdobion/funnelsort/examples/data/MyDataVariable.in' 
    --copy ORIGINAL
    --where 'recordsize >= 39'
    --columnsIn 
      (-n stateName             --offset 26 --length 21 string)
      (-n postalCode            --offset  2 --length  2 string)
    --hexdump(statename)(postalCode)