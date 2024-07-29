package com.example.bigdata.triggers;

import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

public class InstantTrigger extends Trigger<Object, TimeWindow> {
    private static final long serialVersionUID = 1L;

    private InstantTrigger() {
    }

    public TriggerResult onElement(Object element, long timestamp, TimeWindow window, TriggerContext ctx) throws Exception {
        if (window.maxTimestamp() > ctx.getCurrentWatermark()) {
            ctx.registerEventTimeTimer(window.maxTimestamp());
        }
        return TriggerResult.FIRE;
    }

    public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) {
        return time == window.maxTimestamp() ? TriggerResult.FIRE : TriggerResult.CONTINUE;
    }

    public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
        return TriggerResult.CONTINUE;
    }

    public void clear(TimeWindow window, TriggerContext ctx) throws Exception {
        ctx.deleteEventTimeTimer(window.maxTimestamp());
    }

    public boolean canMerge() {
        return true;
    }

    public void onMerge(TimeWindow window, OnMergeContext ctx) {
        long windowMaxTimestamp = window.maxTimestamp();
        if (windowMaxTimestamp > ctx.getCurrentWatermark()) {
            ctx.registerEventTimeTimer(windowMaxTimestamp);
        }

    }

    public String toString() {
        return "InstantTrigger()";
    }

    public static InstantTrigger create() {
        return new InstantTrigger();
    }
}
