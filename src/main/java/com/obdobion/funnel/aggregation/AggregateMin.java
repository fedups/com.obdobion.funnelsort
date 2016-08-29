package com.obdobion.funnel.aggregation;

import java.time.LocalDateTime;
import java.util.Calendar;

import com.obdobion.calendar.CalendarFactory;
import com.obdobion.funnel.orderby.KeyPart;
import com.obdobion.funnel.parameters.FunnelContext;

/**
 * <p>
 * AggregateMin class.
 * </p>
 *
 * @author Chris DeGreef fedupforone@gmail.com
 */
public class AggregateMin extends Aggregate
{
    double   minDouble;
    long     minLong;
    Calendar minCalendar;

    /**
     * <p>
     * Constructor for AggregateMin.
     * </p>
     */
    public AggregateMin()
    {
        reset();
    }

    @Override
    Object getValueForEquations()
    {
        if (minDouble != Double.MAX_VALUE)
            return new Double(minDouble);
        if (minLong != Long.MAX_VALUE)
            return new Long(minLong);
        if (minCalendar != null)
            return minCalendar;
        return new Double(0);
    }

    @Override
    void reset()
    {
        minDouble = Double.MAX_VALUE;
        minLong = Long.MAX_VALUE;
        minCalendar = null;
    }

    @Override
    void update(final FunnelContext context) throws Exception
    {
        if (equation != null)
        {
            final Object unknownType = equation.evaluate();
            if (unknownType instanceof Double)
            {
                final double currentValue = ((Double) unknownType).doubleValue();
                if (currentValue < minDouble)
                    minDouble = currentValue;
                return;
            }
            if (unknownType instanceof Long)
            {
                final long currentValue = ((Long) unknownType).longValue();
                if (currentValue < minLong)
                    minLong = currentValue;
                return;
            }
            if (unknownType instanceof LocalDateTime)
            {
                final Calendar currentValue = CalendarFactory.asCalendar((LocalDateTime) unknownType);
                if (minCalendar == null || currentValue.before(minCalendar))
                    minCalendar = currentValue;
                return;
            }
            minDouble = 0;
            return;
        }
        if (columnName != null)
        {
            final KeyPart col = context.columnHelper.get(columnName);
            if (col.isDate())
            {
                final Calendar currentValue = (Calendar) col.getContents();
                if (minCalendar == null || currentValue.before(minCalendar))
                    minCalendar = currentValue;
                return;
            }
            if (col.isFloat())
            {
                final double currentValue = col.getContentsAsDouble();
                if (currentValue < minDouble)
                    minDouble = currentValue;
                return;
            }
            if (col.isInteger())
            {
                final long currentValue = (Long) col.getContents();
                if (currentValue < minLong)
                    minLong = currentValue;
                return;
            }
        }
        minDouble = 0;
    }
}
