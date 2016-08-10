package com.obdobion.funnel;

import java.io.IOException;
import java.text.ParseException;

/**
 * <p>FunnelDataProvider interface.</p>
 *
 * @author Chris DeGreef fedupforone@gmail.com
 */
public interface FunnelDataProvider
{
    /**
     * <p>actualNumberOfRows.</p>
     *
     * @return a long.
     */
    public long actualNumberOfRows ();

    /**
     * <p>attachTo.</p>
     *
     * @param item a {@link com.obdobion.funnel.FunnelItem} object.
     */
    public void attachTo (FunnelItem item);

    /**
     * <p>close.</p>
     *
     * @throws java.io.IOException if any.
     * @throws java.text.ParseException if any.
     */
    public void close () throws IOException, ParseException;

    /**
     * <p>maximumNumberOfRows.</p>
     *
     * @return a long.
     */
    public long maximumNumberOfRows ();

    /**
     * <p>next.</p>
     *
     * @param item a {@link com.obdobion.funnel.FunnelItem} object.
     * @param phase a long.
     * @return a boolean.
     * @throws java.io.IOException if any.
     * @throws java.text.ParseException if any.
     */
    public boolean next (FunnelItem item, long phase) throws IOException, ParseException;

    /**
     * <p>reset.</p>
     *
     * @throws java.io.IOException if any.
     * @throws java.text.ParseException if any.
     */
    public void reset () throws IOException, ParseException;
}
