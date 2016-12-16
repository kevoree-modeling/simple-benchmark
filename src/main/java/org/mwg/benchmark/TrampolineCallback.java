package org.mwg.benchmark;

import org.mwg.*;
import org.mwg.core.scheduler.TrampolineScheduler;
import org.mwg.memory.offheap.OffHeapMemoryPlugin;
import org.mwg.plugin.Job;
import org.mwg.plugin.SchedulerAffinity;

import java.util.concurrent.atomic.AtomicInteger;

public class TrampolineCallback {

    public static void main(String[] args) {

        boolean isOffHeap = false;
        if (args.length > 0 && args[0].equals("offheap")) {
            isOffHeap = true;
        }
        final GraphBuilder builder = GraphBuilder
                .newBuilder()
                .withScheduler(new TrampolineScheduler())
                .withMemorySize(8000000);
        if (isOffHeap) {
            builder.withPlugin(new OffHeapMemoryPlugin());
        }
        final Graph g = builder.build();

        g.connect(new Callback<Boolean>() {
            public void on(Boolean result) {
                final int times = 1000000;
                final Node user = g.newNode(0, 0);
                user.set("name", Type.STRING, "hello");
                final Node position = g.newNode(0, 0);
                user.addToRelation("position", position);
                final long before = System.currentTimeMillis();
                final DeferCounter defer = g.newCounter(times);
                final AtomicInteger timeCounter = new AtomicInteger(0);
                insert(position, timeCounter, defer, times);
                defer.then(new Job() {
                    public void run() {
                        final long afterInsert = System.currentTimeMillis();
                        double insertTimeSecond = (afterInsert - before) / 1000d;
                        System.out.println("\tinsert " + times / insertTimeSecond + " ops/s");
                        final DeferCounter readFlat = g.newCounter(times);
                        timeCounter.set(0);//reset counter
                        final AtomicInteger fakeSum = new AtomicInteger();
                        read(position, timeCounter, readFlat, times, fakeSum);
                        readFlat.then(new Job() {
                            public void run() {
                                final long afterRead = System.currentTimeMillis();
                                double insertTimeSecond = (afterRead - afterInsert) / 1000d;
                                System.out.println("\twithoutRelation " + times / insertTimeSecond + " ops/s");
                                timeCounter.set(0);//reset counter
                                fakeSum.set(0);
                                final DeferCounter readRelation = g.newCounter(times);
                                readAndTraverse(user, timeCounter, readRelation, times, fakeSum);
                                readRelation.then(new Job() {
                                    public void run() {
                                        long afterReadRelation = System.currentTimeMillis();
                                        double insertTimeSecond = (afterReadRelation - afterRead) / 1000d;
                                        System.out.println("\twithRelation " + (times / insertTimeSecond) + " ops/s");
                                    }
                                });

                            }
                        });
                    }
                });
            }
        });
    }

    private static void insert(final Node position, final AtomicInteger counter, final DeferCounter defer, final int max) {
        final int time = counter.incrementAndGet();
        position.travelInTime(time, new Callback<Node>() {
            public void on(Node timedNode) {
                timedNode.set("lat", Type.DOUBLE, time + 10.5);
                timedNode.set("long", Type.DOUBLE, time + 10.5);
                defer.count();
                if (time != max) {
                    position.graph().scheduler().dispatch(SchedulerAffinity.SAME_THREAD, new Job() {
                        public void run() {
                            insert(position, counter, defer, max);
                        }
                    });
                }
            }
        });
    }

    private static void read(final Node position, final AtomicInteger counter, final DeferCounter defer, final int max, final AtomicInteger fakeSum) {
        final int time = counter.incrementAndGet();
        position.travelInTime(time, new Callback<Node>() {
            public void on(Node timedNode) {
                fakeSum.addAndGet(((Double) timedNode.get("lat")).intValue());
                fakeSum.addAndGet(((Double) timedNode.get("long")).intValue());
                timedNode.free(); // optional if we want to keep everything in memory
                defer.count();
                if (time != max) {
                    position.graph().scheduler().dispatch(SchedulerAffinity.SAME_THREAD, new Job() {
                        public void run() {
                            read(position, counter, defer, max, fakeSum);
                        }
                    });
                }
            }
        });
    }

    private static void readAndTraverse(final Node user, final AtomicInteger counter, final DeferCounter defer, final int max, final AtomicInteger fakeSum) {
        final int time = counter.incrementAndGet();
        user.travelInTime(time, new Callback<Node>() {
            public void on(final Node timeUser) {
                timeUser.relation("position", new Callback<Node[]>() {
                    public void on(Node[] timedPositions) {
                        final Node position = timedPositions[0];
                        fakeSum.addAndGet(((Double) position.get("lat")).intValue());
                        fakeSum.addAndGet(((Double) position.get("long")).intValue());

                        user.graph().freeNodes(timedPositions); //optional if we want to keep everything in memory
                        timeUser.free(); //optional if we want to keep everything in memory

                        defer.count();
                        if (time != max) {
                            position.graph().scheduler().dispatch(SchedulerAffinity.SAME_THREAD, new Job() {
                                public void run() {
                                    readAndTraverse(user, counter, defer, max, fakeSum);
                                }
                            });
                        }
                    }
                });
            }
        });
    }


}
