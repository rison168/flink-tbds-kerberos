package com.tbds.rison.trigger;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.ContinuousProcessingTimeTrigger;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.Window;

/**
 * @PACKAGE_NAME: com.tbds.rison.func
 * @NAME: CustomTrigger
 * @USER: Rison
 * @DATE: 2022/7/19 9:19
 * @PROJECT_NAME: flink-tbds-kerberos
 **/
public class CustomTrigger<W extends Window> extends Trigger<Object, W> {
    private long maxCount;
    private long interval = 60 * 1000;
    private ReducingStateDescriptor<Long> countStateDescriptor;
    private ReducingStateDescriptor<Long> processTimeStateDescriptor;
    private ReducingStateDescriptor<Long> eventTimeStateDescriptor;


    public CustomTrigger(long maxCount) {
        this.countStateDescriptor = new ReducingStateDescriptor<Long>(
                "counter",
                new CustomTrigger.Sum(),
                LongSerializer.INSTANCE);

        this.processTimeStateDescriptor = new ReducingStateDescriptor<Long>(
                "processTimer",
                new CustomTrigger.Update(),
                LongSerializer.INSTANCE);

        this.eventTimeStateDescriptor = new ReducingStateDescriptor<Long>(
                "eventTimer",
                new CustomTrigger.Update(),
                LongSerializer.INSTANCE);

        this.maxCount = maxCount;
    }

    public CustomTrigger(long maxCount, long interval) {
        this.countStateDescriptor = new ReducingStateDescriptor<Long>(
                "counter",
                new CustomTrigger.Sum(),
                LongSerializer.INSTANCE);

        this.processTimeStateDescriptor = new ReducingStateDescriptor<Long>(
                "processTimer",
                new CustomTrigger.Update(),
                LongSerializer.INSTANCE);

        this.eventTimeStateDescriptor = new ReducingStateDescriptor<Long>(
                "eventTimer",
                new CustomTrigger.Update(),
                LongSerializer.INSTANCE);

        this.maxCount = maxCount;
        this.interval = interval;
    }

    @Override
    public TriggerResult onElement(Object o, long l, Window window, TriggerContext triggerContext) throws Exception {
        ReducingState<Long> countState = triggerContext.getPartitionedState(this.countStateDescriptor);
        ReducingState<Long> eventTimerState = triggerContext.getPartitionedState(this.eventTimeStateDescriptor);
        ReducingState<Long> processTimerState = triggerContext.getPartitionedState(this.processTimeStateDescriptor);
        countState.add(1L);
        if (eventTimerState.get() == null) {
            eventTimerState.add(window.maxTimestamp());
            triggerContext.registerEventTimeTimer(window.maxTimestamp());
        }
        if (countState.get().longValue() >= this.maxCount) {
            triggerContext.deleteProcessingTimeTimer(processTimerState.get());
            countState.clear();
            return TriggerResult.FIRE;
        } else if (processTimerState.get() == null) {
            processTimerState.add(triggerContext.getCurrentProcessingTime() + interval);
            triggerContext.registerProcessingTimeTimer(processTimerState.get());
            return TriggerResult.CONTINUE;
        }
        return TriggerResult.CONTINUE;
    }

    @Override
    public TriggerResult onProcessingTime(long l, Window window, TriggerContext triggerContext) throws Exception {
        ReducingState<Long> countState = triggerContext.getPartitionedState(this.countStateDescriptor);
        ReducingState<Long> processTimerState = triggerContext.getPartitionedState(this.processTimeStateDescriptor);
        if (processTimerState.get().longValue() > 0L && processTimerState.get().longValue() == l) {
            countState.clear();
            processTimerState.clear();
            return TriggerResult.FIRE;
        }
        return TriggerResult.CONTINUE;
    }

    @Override
    public TriggerResult onEventTime(long l, Window window, TriggerContext triggerContext) throws Exception {
        ReducingState<Long> countState = triggerContext.getPartitionedState(this.countStateDescriptor);
        ReducingState<Long> eventTimerState = triggerContext.getPartitionedState(this.eventTimeStateDescriptor);
        if (l >= window.maxTimestamp() && countState.get().longValue() > 0L) {
            eventTimerState.clear();
            return TriggerResult.FIRE_AND_PURGE;
        } else if (l >= window.maxTimestamp() && countState.get().longValue() == 0L) {
            return TriggerResult.PURGE;
        }
        return TriggerResult.CONTINUE;
    }

    @Override
    public void clear(Window window, TriggerContext triggerContext) throws Exception {
        ReducingState<Long> countState = triggerContext.getPartitionedState(this.countStateDescriptor);
        ReducingState<Long> eventTimerState = triggerContext.getPartitionedState(this.eventTimeStateDescriptor);
        ReducingState<Long> processTimerState = triggerContext.getPartitionedState(this.processTimeStateDescriptor);

        triggerContext.deleteEventTimeTimer(eventTimerState.get());
        triggerContext.deleteProcessingTimeTimer(processTimerState.get());
        countState.clear();
        eventTimerState.clear();
        processTimerState.clear();

    }

    public static <W extends Window> CustomTrigger<W> of(Long maxCount, Time interval) {
        return new CustomTrigger(maxCount, interval.toMilliseconds());
    }

    private static class Sum implements ReduceFunction<Long> {
        public Sum() {
        }

        @Override
        public Long reduce(Long aLong, Long t1) throws Exception {
            return aLong + t1;
        }
    }

    private static class Update implements ReduceFunction<Long> {
        public Update() {
        }

        @Override
        public Long reduce(Long aLong, Long t1) throws Exception {
            return t1;
        }
    }


}

