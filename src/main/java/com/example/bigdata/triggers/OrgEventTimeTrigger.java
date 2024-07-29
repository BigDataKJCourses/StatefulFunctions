package com.example.bigdata.triggers;

import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

public class OrgEventTimeTrigger extends Trigger<Object, TimeWindow> {
    private static final long serialVersionUID = 1L;

    private OrgEventTimeTrigger() {
    }

    public TriggerResult onElement(Object element, long timestamp, TimeWindow window, Trigger.TriggerContext ctx) throws Exception {
        if (window.maxTimestamp() <= ctx.getCurrentWatermark()) {
            return TriggerResult.FIRE;
        } else {
            ctx.registerEventTimeTimer(window.maxTimestamp());
            return TriggerResult.CONTINUE;
        }
    }

    public TriggerResult onEventTime(long time, TimeWindow window, Trigger.TriggerContext ctx) {
        return time == window.maxTimestamp() ? TriggerResult.FIRE : TriggerResult.CONTINUE;
    }

    public TriggerResult onProcessingTime(long time, TimeWindow window, Trigger.TriggerContext ctx) throws Exception {
        return TriggerResult.CONTINUE;
    }

    public void clear(TimeWindow window, Trigger.TriggerContext ctx) throws Exception {
        ctx.deleteEventTimeTimer(window.maxTimestamp());
    }

    public boolean canMerge() {
        return true;
    }

    public void onMerge(TimeWindow window, Trigger.OnMergeContext ctx) {
        long windowMaxTimestamp = window.maxTimestamp();
        if (windowMaxTimestamp > ctx.getCurrentWatermark()) {
            ctx.registerEventTimeTimer(windowMaxTimestamp);
        }

    }

    public String toString() {
        return "OrgEventTimeTrigger()";
    }

    public static OrgEventTimeTrigger create() {
        return new OrgEventTimeTrigger();
    }
}
