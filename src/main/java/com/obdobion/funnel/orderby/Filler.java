package com.obdobion.funnel.orderby;

public class Filler extends KeyPart
{

    @Override
    public Object getContents()
    {
        return null;
    }

    @Override
    public double getContentsAsDouble()
    {
        return 0;
    }

    @Override
    public boolean isNumeric()
    {
        return false;
    }

    /**
     * @param context
     */
    @Override
    public void pack(final KeyContext context) throws Exception
    {
        // n/a
    }

    /**
     * @param rawData
     */
    @Override
    public void parseObjectFromRawData(final byte[] rawData) throws Exception
    {
        // n/a
    }
}
