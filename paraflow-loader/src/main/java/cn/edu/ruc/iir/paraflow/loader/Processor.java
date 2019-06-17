package cn.edu.ruc.iir.paraflow.loader;

import java.util.concurrent.atomic.AtomicBoolean;

public abstract class Processor
{
    final String name;
    final String db;
    final String table;
    final AtomicBoolean isReadyToStop = new AtomicBoolean(false);
    private final int parallelism;

    public Processor(String name, String db, String table, int parallelism)
    {
        this.name = name;
        this.db = db;
        this.table = table;
        this.parallelism = parallelism;
    }

    public abstract void run();

    public String getName()
    {
        return name;
    }

    public String getDb()
    {
        return db;
    }

    public String getTable()
    {
        return table;
    }

    public int getParallelism()
    {
        return parallelism;
    }

    public void stop()
    {
        isReadyToStop.set(true);
    }
}
