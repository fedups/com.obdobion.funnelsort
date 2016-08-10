package com.obdobion.funnel.columns;

import java.io.IOException;

/**
 * <p>ColumnWriter interface.</p>
 *
 * @author Chris DeGreef fedupforone@gmail.com
 */
public interface ColumnWriter
{
    /**
     * <p>write.</p>
     *
     * @param sourceBytes an array of byte.
     * @param off a int.
     * @param len a int.
     * @throws java.io.IOException if any.
     */
    public void write (byte[] sourceBytes, int off, int len) throws IOException;
}
