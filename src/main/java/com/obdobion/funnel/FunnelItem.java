package com.obdobion.funnel;

import java.io.IOException;
import java.text.ParseException;

import com.obdobion.funnel.segment.SourceProxyRecord;

/**
 * @author Chris DeGreef
 *
 */
public class FunnelItem
{
    FunnelDataProvider provider;
    SourceProxyRecord  data;
    boolean            endOfData;
    long               phase;

    public SourceProxyRecord getData ()
    {

        return data;
    }

    public long getPhase ()
    {
        return phase;
    }

    public FunnelDataProvider getProvider ()
    {
        return provider;
    }

    public boolean isEndOfData ()
    {
        return endOfData;
    }

    public boolean next (final long _phase) throws IOException, ParseException
    {
        if (!provider.next(this, _phase))
        {
            this.setEndOfData(true);
            return false;
        }
        return true;
    }

    public void reset ()
    {
        data = null;
        endOfData = false;
        phase = -1;
    }

    public void setData (final SourceProxyRecord _data)
    {
        this.data = _data;
    }

    public void setEndOfData (final boolean _endOfData)
    {
        this.endOfData = _endOfData;
    }

    public void setPhase (final long _phase)
    {
        this.phase = _phase;
    }

    public void setProvider (final FunnelDataProvider _provider)
    {
        this.provider = _provider;
    }

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