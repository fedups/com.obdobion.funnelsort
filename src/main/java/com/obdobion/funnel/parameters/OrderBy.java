package com.obdobion.funnel.parameters;

import com.obdobion.argument.annotation.Arg;
import com.obdobion.funnel.orderby.KeyDirection;

/**
 * <p>
 * OrderBy class.
 * </p>
 *
 * @author Chris DeGreef fedupforone@gmail.com
 */
public class OrderBy
{
    @Arg(positional = true,
            required = true,
            allowCamelCaps = true,
            help = "A previously defined column name.")
    public String       columnName;

    @Arg(shortName = 'd',
            positional = true,
            defaultValues = "ASC",
            help = "The direction of the sort for this key. AASC and ADESC are absolute values of the key - the case of letters would not matter and the sign of numbers would not matter.")
    public KeyDirection direction;

    /**
     * <p>
     * Constructor for OrderBy.
     * </p>
     */
    public OrderBy()
    {
        super();
    }

}
