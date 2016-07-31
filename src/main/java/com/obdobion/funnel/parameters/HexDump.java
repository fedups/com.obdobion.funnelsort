package com.obdobion.funnel.parameters;

import com.obdobion.argument.annotation.Arg;

/**
 * @author Chris DeGreef
 *
 */
public class HexDump
{
    @Arg(positional = true, allowCamelCaps = true, help = "A previously defined column name.")
    public String columnName;

    public HexDump()
    {
        super();
    }

}