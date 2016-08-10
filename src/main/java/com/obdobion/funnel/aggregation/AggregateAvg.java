package com.obdobion.funnel.aggregation;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Calendar;

import com.obdobion.funnel.orderby.KeyPart;
import com.obdobion.funnel.parameters.FunnelContext;

/**
 * <p>AggregateAvg class.</p>
 *
 * @author Chris DeGreef fedupforone@gmail.com
 */
public class AggregateAvg extends Aggregate
{
    double totalDouble;
    long   totalLong;
    long   occurrences;
    long   totalCalendar;

    long   overflowCount;

    /**
     * <p>Constructor for AggregateAvg.</p>
     */
    public AggregateAvg()
    {
        reset();
    }

    @Override
    Object getValueForEquations ()
    {
        if (occurrences == 0)
            return new Double(0);

        if (aggType == AggType.FLOAT)
        {
            if (overflowCount != 0)
            {
                BigDecimal answer = new BigDecimal(Double.MAX_VALUE);
                answer = answer.multiply(new BigDecimal(overflowCount));
                answer = answer.add(new BigDecimal(totalDouble));
                answer = answer.divide(new BigDecimal(occurrences), RoundingMode.DOWN);
                return new Double(answer.doubleValue());
            }
            return new Double(totalDouble / occurrences);
        }
        if (aggType == AggType.INT)
        {
            if (overflowCount != 0)
            {
                BigDecimal answer = new BigDecimal(Long.MAX_VALUE);
                answer = answer.multiply(new BigDecimal(overflowCount));
                answer = answer.add(new BigDecimal(totalLong));
                answer = answer.divide(new BigDecimal(occurrences), RoundingMode.DOWN);
                return new Long(answer.longValue());
            }
            return new Long(totalLong / occurrences);
        }
        if (aggType == AggType.CAL)
        {
            long bigResult;
            if (overflowCount > 0)
            {
                BigDecimal answer = new BigDecimal(Long.MAX_VALUE);
                answer = answer.multiply(new BigDecimal(overflowCount));
                answer = answer.add(new BigDecimal(totalLong));
                answer = answer.divide(new BigDecimal(occurrences));
                bigResult = answer.longValue();
            } else
                bigResult = totalCalendar;

            final Calendar cal = Calendar.getInstance();
            cal.setTimeInMillis(bigResult / occurrences);
            return cal;
        }

        return new Double(0);
    }

    @Override
    void reset ()
    {
        totalDouble = 0;
        totalLong = 0;
        totalCalendar = 0;
        occurrences = 0;
        overflowCount = 0;
    }

    @Override
    void update (final FunnelContext context) throws Exception
    {
        occurrences++;

        if (equation != null)
        {
            final Object unknownType = equation.evaluate();
            if (unknownType instanceof Double)
            {
                aggType = AggType.FLOAT;
                final double currentValue = ((Double) unknownType).doubleValue();
                if (Double.MAX_VALUE - currentValue < totalDouble)
                {
                    overflowCount++;
                    totalDouble = currentValue - (Double.MAX_VALUE - totalDouble);
                } else
                    totalDouble += currentValue;
                return;
            }
            if (unknownType instanceof Long)
            {
                aggType = AggType.INT;
                final long currentValue = ((Long) unknownType).longValue();
                if (Long.MAX_VALUE - currentValue < totalLong)
                {
                    overflowCount++;
                    totalLong = currentValue - (Long.MAX_VALUE - totalLong);
                } else
                    totalLong += currentValue;
                return;
            }
            if (unknownType instanceof Calendar)
            {
                aggType = AggType.CAL;
                final long currentValue = ((Calendar) unknownType).getTimeInMillis();
                if (Long.MAX_VALUE - currentValue < totalCalendar)
                {
                    overflowCount++;
                    totalCalendar = currentValue - (Long.MAX_VALUE - totalCalendar);
                } else
                    totalCalendar += currentValue;
                return;
            }
            return;
        }
        if (columnName != null)
        {
            final KeyPart col = context.columnHelper.get(columnName);
            if (col.isDate())
            {
                aggType = AggType.CAL;
                final long currentValue = ((Calendar) col.getContents()).getTimeInMillis();
                if (Long.MAX_VALUE - currentValue < totalCalendar)
                {
                    overflowCount++;
                    totalCalendar = currentValue - (Long.MAX_VALUE - totalCalendar);
                } else
                    totalCalendar += currentValue;
                return;
            }
            if (col.isFloat())
            {
                aggType = AggType.FLOAT;
                final double currentValue = col.getContentsAsDouble();
                if (Double.MAX_VALUE - currentValue < totalDouble)
                {
                    overflowCount++;
                    totalDouble = currentValue - (Double.MAX_VALUE - totalDouble);
                } else
                    totalDouble += currentValue;
                return;
            }
            if (col.isInteger())
            {
                aggType = AggType.INT;
                final long currentValue = (Long) col.getContents();
                if (totalLong >= 0 && currentValue > 0)
                {
                    if (Long.MAX_VALUE - currentValue < totalLong)
                    {
                        overflowCount++;
                        totalLong = currentValue - (Long.MAX_VALUE - totalLong);
                    } else
                        totalLong += currentValue;
                    return;
                }
                if (totalLong >= 0 && currentValue < 0)
                {
                    totalLong += currentValue;
                    return;
                }
                if (totalLong < 0 && currentValue > 0)
                {
                    totalLong += currentValue;
                    return;
                }
                if (totalLong < 0 && currentValue < 0)
                {
                    if (-(Long.MAX_VALUE + totalLong) > currentValue)
                    {
                        totalLong = currentValue - (Long.MAX_VALUE + totalLong);
                        overflowCount--;
                    } else
                        totalLong += currentValue;
                    return;
                }
                return;
            }
        }
    }
}
