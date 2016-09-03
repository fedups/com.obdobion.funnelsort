package com.obdobion.funnel.provider;

import java.io.IOException;

import com.obdobion.funnel.FunnelDataProvider;
import com.obdobion.funnel.FunnelItem;

/**
 * <p>
 * EmptyProvider class.
 * </p>
 *
 * @author Chris DeGreef fedupforone@gmail.com
 */
public class EmptyProvider implements FunnelDataProvider
{
    /** {@inheritDoc} */
    @Override
    public long actualNumberOfRows()
    {
        return 0;
    }

    /** {@inheritDoc} */
    @Override
    public void attachTo(final FunnelItem item)
    {
        // intentionally empty
    }

    /** {@inheritDoc} */
    @Override
    public void close() throws IOException
    {
        // intentionally empty
    }

    /** {@inheritDoc} */
    @Override
    public long maximumNumberOfRows()
    {
        return Long.MAX_VALUE;
    }

    /** {@inheritDoc} */
    @Override
    public boolean next(final FunnelItem item, final long phase)
    {
        item.setData(null);
        item.setEndOfData(true);
        return false;
    }

    /** {@inheritDoc} */
    @Override
    public void reset()
    {
        // Intentionally empty
    }
}
