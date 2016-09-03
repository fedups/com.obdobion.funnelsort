package com.obdobion.funnel.aggregation;

import com.obdobion.funnel.orderby.KeyPart;
import com.obdobion.funnel.parameters.FunnelContext;

/**
 * <p>
 * AggregateSum class.
 * </p>
 *
 * @author Chris DeGreef fedupforone@gmail.com
 */
public class AggregateSum extends Aggregate
{
    double sumDouble;
    long   sumLong;

    /**
     * <p>
     * Constructor for AggregateSum.
     * </p>
     */
    public AggregateSum()
    {
        reset();
    }

    @Override
    Object getValueForEquations()
    {
        if (AggType.FLOAT == aggType)
            return new Double(sumDouble);
        if (AggType.INT == aggType)
            return new Long(sumLong);
        return new Double(0);
    }

    @Override
    void reset()
    {
        sumDouble = 0D;
        sumLong = 0L;
    }

    @Override
    void update(final FunnelContext context) throws Exception
    {
        if (equation != null)
        {
            final Object unknownType = equation.evaluate();
            if (unknownType instanceof Double)
            {
                aggType = AggType.FLOAT;
                final double currentValue = ((Double) unknownType).doubleValue();
                sumDouble += currentValue;
                return;
            }
            if (unknownType instanceof Long)
            {
                aggType = AggType.INT;
                final long currentValue = ((Long) unknownType).longValue();
                sumLong += currentValue;
                return;
            }
            return;
        }
        if (columnName != null)
        {
            final KeyPart col = context.columnHelper.get(columnName);
            if (col.isFloat())
            {
                aggType = AggType.FLOAT;
                final double currentValue = col.getContentsAsDouble();
                sumDouble += currentValue;
                return;
            }
            if (col.isInteger())
            {
                aggType = AggType.INT;
                final long currentValue = (Long) col.getContents();
                sumLong += currentValue;
                return;
            }
        }
    }
}
