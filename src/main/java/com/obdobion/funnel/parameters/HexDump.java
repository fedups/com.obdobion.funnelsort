package com.obdobion.funnel.parameters;

import com.obdobion.argument.annotation.Arg;

/**
 * <p>
 * HexDump class.
 * </p>
 *
 * @author Chris DeGreef fedupforone@gmail.com
 */
public class HexDump
{
    @Arg(positional = true, allowCamelCaps = true, help = "A previously defined column name.")
    public String columnName;

    /**
     * <p>
     * Constructor for HexDump.
     * </p>
     */
    public HexDump()
    {
        super();
    }

}
