package com.obdobion.funnel.aggregation;

import com.obdobion.funnel.parameters.FunnelContext;

/**
 * <p>AggregateCount class.</p>
 *
 * @author Chris DeGreef fedupforone@gmail.com
 */
public class AggregateCount extends Aggregate
{
    protected long counter;

    @Override
    Object getValueForEquations()
    {
        return new Long(counter);
    }

    @Override
    void reset()
    {
        counter = 0;
    }

    /** {@inheritDoc} */
    @Override
    public boolean supportsDate()
    {
        return false;
    }

    /** {@inheritDoc} */
    @Override
    public boolean supportsNumber()
    {
        return false;
    }

    /**
     * @param context
     */
    @Override
    void update(final FunnelContext context)
    {
        counter++;
    }

}
