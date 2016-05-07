package com.obdobion.funnel.columns;

import java.io.IOException;

public interface ColumnWriter
{
    public void write (byte[] sourceBytes, int off, int len) throws IOException;
}
