package com.datafibers.flink.table_api.util;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

import javax.annotation.Nullable;

public class WatermarkAssigner  implements AssignerWithPeriodicWatermarks<Tuple2<String, Long>> {

    @Nullable
    private long maxTimestamp = 0;
    public Watermark getCurrentWatermark() {
        return new Watermark(maxTimestamp);
    }

    public long extractTimestamp(Tuple2<String, Long> item, long l) {
        maxTimestamp = Math.max(maxTimestamp, item.f1);
        return item.f1;
    }
}
