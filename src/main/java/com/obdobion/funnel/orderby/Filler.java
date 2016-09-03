package com.obdobion.funnel.orderby;

/**
 * <p>
 * Filler class.
 * </p>
 *
 * @author Chris DeGreef fedupforone@gmail.com
 */
public class Filler extends KeyPart
{

    /** {@inheritDoc} */
    @Override
    public Object getContents()
    {
        return null;
    }

    /** {@inheritDoc} */
    @Override
    public double getContentsAsDouble()
    {
        return 0;
    }

    /** {@inheritDoc} */
    @Override
    public boolean isNumeric()
    {
        return false;
    }

    /** {@inheritDoc} */
    @Override
    public void pack(final KeyContext context) throws Exception
    {
        // n/a
    }

    /** {@inheritDoc} */
    @Override
    public void parseObjectFromRawData(final byte[] rawData) throws Exception
    {
        // n/a
    }
}
