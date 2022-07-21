package com.tbds.rison.trigger;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.Window;

/**
 * @PACKAGE_NAME: com.tbds.rison.trigger
 * @NAME: HbaseTrigger
 * @USER: Rison
 * @DATE: 2022/7/20 11:12
 * @PROJECT_NAME: flink-tbds-kerberos
 **/
public class UserTrigger<W extends Window> extends Trigger<Object, W> {
    private long threshold = 1000;
    private ReducingStateDescriptor<Long> countDesc;

    public UserTrigger() {
        this.countDesc = new ReducingStateDescriptor<Long>(
                "counter",
                new UserTrigger.Sum(),
                LongSerializer.INSTANCE);
    }

    public UserTrigger(long threshold) {
        this.countDesc = new ReducingStateDescriptor<Long>(
                "counter",
                new UserTrigger.Sum(),
                LongSerializer.INSTANCE);
        this.threshold = threshold;
    }

    public static  <W extends Window> UserTrigger<W> of(long threshold) {
        return new UserTrigger(threshold);
    }

    @Override
    public TriggerResult onElement(Object o, long l, W w, TriggerContext triggerContext) throws Exception {
        ReducingState<Long> countState = triggerContext.getPartitionedState(countDesc);
        countState.add(1L);
        triggerContext.registerEventTimeTimer(w.maxTimestamp());
        if (countState.get() >= threshold){
            countState.clear();
            return TriggerResult.FIRE_AND_PURGE;
        }
        return TriggerResult.CONTINUE;
    }

    @Override
    public TriggerResult onProcessingTime(long l, W w, TriggerContext triggerContext) throws Exception {
        if (l == w.maxTimestamp()){
            triggerContext.getPartitionedState(countDesc).clear();
            return TriggerResult.FIRE_AND_PURGE;
        }
        return TriggerResult.CONTINUE;
    }

    @Override
    public TriggerResult onEventTime(long l, W w, TriggerContext triggerContext) throws Exception {
        return TriggerResult.CONTINUE;
    }

    @Override
    public void clear(W w, TriggerContext triggerContext) throws Exception {
        triggerContext.deleteProcessingTimeTimer(w.maxTimestamp());
        triggerContext.getPartitionedState(countDesc).clear();

    }

    private static class Sum implements ReduceFunction<Long> {
        public Sum() {
        }

        @Override
        public Long reduce(Long aLong, Long t1) throws Exception {
            return aLong + t1;
        }
    }
}
