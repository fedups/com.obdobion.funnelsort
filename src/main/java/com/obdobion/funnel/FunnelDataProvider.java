package com.obdobion.funnel;

import java.io.IOException;
import java.text.ParseException;

/**
 * @author Chris DeGreef
 *
 */
public interface FunnelDataProvider
{
    public long actualNumberOfRows ();

    public void attachTo (FunnelItem item);

    public void close () throws IOException, ParseException;

    public long maximumNumberOfRows ();

    public boolean next (FunnelItem item, long phase) throws IOException, ParseException;

    public void reset () throws IOException, ParseException;
}