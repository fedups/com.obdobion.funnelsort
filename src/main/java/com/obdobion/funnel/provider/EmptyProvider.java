package com.obdobion.funnel.provider;

import java.io.IOException;

import com.obdobion.funnel.FunnelDataProvider;
import com.obdobion.funnel.FunnelItem;

/**
 * @author Chris DeGreef
 *
 */
public class EmptyProvider implements FunnelDataProvider
{
    @Override
    public long actualNumberOfRows()
    {
        return 0;
    }

    /**
     * @param item
     */
    @Override
    public void attachTo(final FunnelItem item)
    {
        // intentionally empty
    }

    @Override
    public void close() throws IOException
    {
        // intentionally empty
    }

    @Override
    public long maximumNumberOfRows()
    {
        return Long.MAX_VALUE;
    }

    /**
     * @param phase
     */
    @Override
    public boolean next(final FunnelItem item, final long phase)
    {
        item.setData(null);
        item.setEndOfData(true);
        return false;
    }

    @Override
    public void reset()
    {
        // Intentionally empty
    }
}