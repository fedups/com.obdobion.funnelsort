package com.obdobion.funnel;

import java.io.IOException;
import java.text.ParseException;

import com.obdobion.funnel.segment.SourceProxyRecord;

/**
 * <p>FunnelItem class.</p>
 *
 * @author Chris DeGreef fedupforone@gmail.com
 */
public class FunnelItem
{
    FunnelDataProvider provider;
    SourceProxyRecord  data;
    boolean            endOfData;
    long               phase;

    /**
     * <p>Getter for the field <code>data</code>.</p>
     *
     * @return a {@link com.obdobion.funnel.segment.SourceProxyRecord} object.
     */
    public SourceProxyRecord getData ()
    {

        return data;
    }

    /**
     * <p>Getter for the field <code>phase</code>.</p>
     *
     * @return a long.
     */
    public long getPhase ()
    {
        return phase;
    }

    /**
     * <p>Getter for the field <code>provider</code>.</p>
     *
     * @return a {@link com.obdobion.funnel.FunnelDataProvider} object.
     */
    public FunnelDataProvider getProvider ()
    {
        return provider;
    }

    /**
     * <p>isEndOfData.</p>
     *
     * @return a boolean.
     */
    public boolean isEndOfData ()
    {
        return endOfData;
    }

    /**
     * <p>next.</p>
     *
     * @param _phase a long.
     * @return a boolean.
     * @throws java.io.IOException if any.
     * @throws java.text.ParseException if any.
     */
    public boolean next (final long _phase) throws IOException, ParseException
    {
        if (!provider.next(this, _phase))
        {
            this.setEndOfData(true);
            return false;
        }
        return true;
    }

    /**
     * <p>reset.</p>
     */
    public void reset ()
    {
        data = null;
        endOfData = false;
        phase = -1;
    }

    /**
     * <p>Setter for the field <code>data</code>.</p>
     *
     * @param _data a {@link com.obdobion.funnel.segment.SourceProxyRecord} object.
     */
    public void setData (final SourceProxyRecord _data)
    {
        this.data = _data;
    }

    /**
     * <p>Setter for the field <code>endOfData</code>.</p>
     *
     * @param _endOfData a boolean.
     */
    public void setEndOfData (final boolean _endOfData)
    {
        this.endOfData = _endOfData;
    }

    /**
     * <p>Setter for the field <code>phase</code>.</p>
     *
     * @param _phase a long.
     */
    public void setPhase (final long _phase)
    {
        this.phase = _phase;
    }

    /**
     * <p>Setter for the field <code>provider</code>.</p>
     *
     * @param _provider a {@link com.obdobion.funnel.FunnelDataProvider} object.
     */
    public void setProvider (final FunnelDataProvider _provider)
    {
        this.provider = _provider;
    }

    /** {@inheritDoc} */
    @Override
    public String toString ()
    {
        final StringBuilder sb = new StringBuilder();
        sb.append(provider.getClass().getSimpleName());
        sb.append(" dat=").append(data == null
                ? "null"
                : data.hashCode());
        sb.append(" eod=").append(endOfData);
        sb.append(" pha=").append(phase);
        return sb.toString();
    }
}
