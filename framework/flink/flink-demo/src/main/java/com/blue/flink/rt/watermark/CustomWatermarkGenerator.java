package com.blue.flink.rt.watermark;

import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;

public class CustomWatermarkGenerator<T> implements WatermarkGenerator<T> {
    private long delayTs;
    private long maxTs;

    public CustomWatermarkGenerator(long delayTs) {
        this.delayTs = delayTs;
        this.maxTs = Long.MIN_VALUE+this.delayTs+1;
    }

    /**
     * 每条数据都会调用
     * @param event
     * @param eventTimestamp
     * @param output
     */
    @Override
    public void onEvent(T event, long eventTimestamp, WatermarkOutput output) {
        this.maxTs = Math.max(maxTs,eventTimestamp);
        System.out.println("CustomWatermarkGenerator.onEvent watermark:"+this.maxTs);
    }

    /**
     * 定时调度
     * @param output
     */
    @Override
    public void onPeriodicEmit(WatermarkOutput output) {
        System.out.println("CustomWatermarkGenerator.onPeriodicEmit watermark:"+(this.maxTs-this.delayTs-1));
        output.emitWatermark(new Watermark(this.maxTs-this.delayTs-1));
    }
}