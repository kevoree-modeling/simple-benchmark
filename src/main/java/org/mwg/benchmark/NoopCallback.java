package org.mwg.benchmark;

import org.mwg.*;
import org.mwg.core.scheduler.NoopScheduler;
import org.mwg.memory.offheap.OffHeapMemoryPlugin;
import org.mwg.plugin.Job;
import org.mwg.plugin.SchedulerAffinity;

import java.util.concurrent.atomic.AtomicInteger;

public class NoopCallback {

    public static void main(String[] args) {

        boolean isOffHeap = false;
        if (args.length > 0 && args[0].equals("offheap")) {
            isOffHeap = true;
        }
        final GraphBuilder builder = GraphBuilder
                .newBuilder()
                .withScheduler(new NoopScheduler())
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
                for (int i = 0; i < times; i++) {
                    final int finalI = i;
                    position.travelInTime(i, new Callback<Node>() {
                        public void on(Node timedNode) {
                            timedNode.set("lat", Type.DOUBLE, finalI + 10.5);
                            timedNode.set("long", Type.DOUBLE, finalI + 10.5);
                            timedNode.free();
                        }
                    });
                }
                final long afterInsert = System.currentTimeMillis();
                double insertTimeSecond = (afterInsert - before) / 1000d;
                System.out.println("\tinsert " + times / insertTimeSecond + " ops/s");

                final AtomicInteger fakeSum = new AtomicInteger(0);
                for (int i = 0; i < times; i++) {
                    final int finalI = i;
                    position.travelInTime(finalI, new Callback<Node>() {
                        public void on(Node timedNode) {
                            fakeSum.addAndGet(((Double) timedNode.get("lat")).intValue());
                            fakeSum.addAndGet(((Double) timedNode.get("long")).intValue());
                            timedNode.free(); // optional if we want to keep everything in memory
                        }
                    });
                }
                final long afterRead = System.currentTimeMillis();
                double insertTimeSecondRead = (afterRead - afterInsert) / 1000d;
                System.out.println("\twithoutRelation " + times / insertTimeSecondRead + " ops/s");

                fakeSum.set(0);
                for (int i = 0; i < times; i++) {
                    final int finalI = i;
                    user.travelInTime(finalI, new Callback<Node>() {
                        public void on(final Node timeUser) {
                            timeUser.relation("position", new Callback<Node[]>() {
                                public void on(Node[] timedPositions) {
                                    final Node position = timedPositions[0];
                                    fakeSum.addAndGet(((Double) position.get("lat")).intValue());
                                    fakeSum.addAndGet(((Double) position.get("long")).intValue());

                                    user.graph().freeNodes(timedPositions); //optional if we want to keep everything in memory
                                    timeUser.free(); //optional if we want to keep everything in memory

                                }
                            });
                        }
                    });
                }

                long afterReadRelation = System.currentTimeMillis();
                double insertTimeSecondReadWithRelation = (afterReadRelation - afterRead) / 1000d;
                System.out.println("\twithRelation " + (times / insertTimeSecondReadWithRelation) + " ops/s");
            }
        });
    }

}
