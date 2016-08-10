package com.obdobion.funnel.aggregation;

import java.util.Calendar;

import com.obdobion.funnel.orderby.KeyPart;
import com.obdobion.funnel.parameters.FunnelContext;

/**
 * <p>AggregateMax class.</p>
 *
 * @author Chris DeGreef fedupforone@gmail.com
 */
public class AggregateMax extends Aggregate
{
    double   maxDouble;
    long     maxLong;
    Calendar maxCalendar;

    /**
     * <p>Constructor for AggregateMax.</p>
     */
    public AggregateMax()
    {
        reset();
    }

    @Override
    Object getValueForEquations ()
    {
        if (maxDouble != Double.MIN_VALUE)
            return new Double(maxDouble);
        if (maxLong != Long.MIN_VALUE)
            return new Long(maxLong);
        if (maxCalendar != null)
            return maxCalendar;
        return new Double(0);
    }

    @Override
    void reset ()
    {
        maxDouble = Double.MIN_VALUE;
        maxLong = Long.MIN_VALUE;
        maxCalendar = null;
    }

    @Override
    void update (final FunnelContext context) throws Exception
    {
        if (equation != null)
        {
            final Object unknownType = equation.evaluate();
            if (unknownType instanceof Double)
            {
                final double currentValue = ((Double) unknownType).doubleValue();
                if (currentValue > maxDouble)
                    maxDouble = currentValue;
                return;
            }
            if (unknownType instanceof Long)
            {
                final long currentValue = ((Long) unknownType).longValue();
                if (currentValue > maxLong)
                    maxLong = currentValue;
                return;
            }
            if (unknownType instanceof Calendar)
            {
                final Calendar currentValue = (Calendar) unknownType;
                if (maxCalendar == null || currentValue.after(maxCalendar))
                    maxCalendar = currentValue;
                return;
            }
            maxDouble = 0;
            return;
        }
        if (columnName != null)
        {
            final KeyPart col = context.columnHelper.get(columnName);
            if (col.isDate())
            {
                final Calendar currentValue = (Calendar) col.getContents();
                if (maxCalendar == null || currentValue.after(maxCalendar))
                    maxCalendar = currentValue;
                return;
            }
            if (col.isFloat())
            {
                final double currentValue = col.getContentsAsDouble();
                if (currentValue > maxDouble)
                    maxDouble = currentValue;
                return;
            }
            if (col.isInteger())
            {
                final long currentValue = (Long) col.getContents();
                if (currentValue > maxLong)
                    maxLong = currentValue;
                return;
            }
        }
        maxDouble = 0;
    }
}
