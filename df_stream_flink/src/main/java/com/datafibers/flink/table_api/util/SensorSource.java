package com.datafibers.flink.table_api.util;


import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;

public class SensorSource implements SourceFunction<Tuple4<String, String, Double, Long>> {

    private static final int numSources=3;
    private boolean canceled = false;
    private Random random;

    public SensorSource()
    {
        random = new Random(5);
    }

    @Override
    public void run(SourceContext<Tuple4<String, String, Double, Long>> sourceContext) throws Exception {
        int currentSource=0;
        while (!canceled)
        {
            long ts = System.currentTimeMillis();
            Tuple4 <String, String, Double, Long>  data = new Tuple4<>(String.valueOf(currentSource),String.valueOf(currentSource), random.nextDouble(), ts);
            sourceContext.collectWithTimestamp(data,ts);
            currentSource = (currentSource +1) % numSources;
            long sleepTime = 10;
            Thread.sleep(sleepTime);
        }
    }

    @Override
    public void cancel() {
        canceled = true;
    }
}
