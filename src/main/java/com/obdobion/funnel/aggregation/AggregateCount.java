package com.obdobion.funnel.aggregation;

import com.obdobion.funnel.parameters.FunnelContext;

public class AggregateCount extends Aggregate
{
    protected long counter;

    @Override
    Object getValueForEquations ()
    {
        return new Long(counter);
    }

    @Override
    void reset ()
    {
        counter = 0;
    }

    @Override
    public boolean supportsDate ()
    {
        return false;
    }

    @Override
    public boolean supportsNumber ()
    {
        return false;
    }

    @Override
    void update (final FunnelContext context)
    {
        counter++;
    }

}
