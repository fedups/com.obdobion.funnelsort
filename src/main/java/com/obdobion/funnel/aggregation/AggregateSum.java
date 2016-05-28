package com.obdobion.funnel.aggregation;

import com.obdobion.funnel.orderby.KeyPart;
import com.obdobion.funnel.parameters.FunnelContext;

public class AggregateSum extends Aggregate
{
    double sum;

    @Override
    Object getValueForEquations ()
    {
        return new Double(sum);
    }

    @Override
    void reset ()
    {
        sum = 0;
    }

    @Override
    public boolean supportsDate ()
    {
        return false;
    }

    @Override
    void update (final FunnelContext context) throws Exception
    {
        double currentValue = 0;
        if (equation != null)
        {
            final Object unknownType = equation.evaluate();
            if (unknownType instanceof Double)
                currentValue = ((Double) unknownType).doubleValue();
            else
                currentValue = ((Long) unknownType).doubleValue();
        }
        if (columnName != null)
        {
            final KeyPart col = context.columnHelper.get(columnName);
            currentValue = col.getContentsAsDouble();
        }
        sum += currentValue;
    }
}
