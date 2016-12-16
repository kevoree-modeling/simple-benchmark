package org.mwg.benchmark;

import org.mwg.*;
import org.mwg.core.scheduler.TrampolineScheduler;
import org.mwg.memory.offheap.OffHeapMemoryPlugin;
import org.mwg.task.Task;

import static org.mwg.core.task.Actions.newTask;

public class TrampolineTask {

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
                Task t = newTask();
                t.createNode().defineAsGlobalVar("positionPoint");
                t.createNode().setAttribute("name", Type.STRING, "hello").defineAsGlobalVar("user");
                t.addVarToRelation("position", "positionPoint");
                t.readVar("positionPoint");
                t.thenDo((ctx) -> ctx.setGlobalVariable("before", System.currentTimeMillis()).continueTask());
                //step insert
                t.loop("0", "" + times,
                        newTask().travelInTime("{{i}}").thenDo(ctx -> {
                            ctx.resultAsNodes().get(0)
                                    // .set("lat", Type.DOUBLE, "{{=i+10.5}}") //allow this writing
                                    .set("lat", Type.DOUBLE, ((Integer) ctx.variable("i").get(0)) + 10.5)
                                    .set("long", Type.DOUBLE, ((Integer) ctx.variable("i").get(0)) + 10.5);
                            //System.out.println(ctx.variable("i").get(0) + "->" + ctx.resultAsNodes().get(0));
                            ctx.continueTask();
                        })
                );
                t.thenDo(ctx -> {
                    long afterInsert = System.currentTimeMillis();
                    ctx.setGlobalVariable("afterInsert", afterInsert);
                    long before = (Long) ctx.variable("before").get(0);
                    double timeInSeconds = (afterInsert - before) / 1000d;
                    System.out.println("\tinsert " + times / timeInSeconds + " ops/s");
                    ctx.continueTask();
                });
                //step read position
                t.loop("0", "" + times,
                        newTask().travelInTime("{{i}}").isolate(newTask().attribute("lat").attribute("long"))
                );
                t.thenDo(ctx -> {
                    long afterRead = System.currentTimeMillis();
                    ctx.setGlobalVariable("afterRead", afterRead);
                    long before = (Long) ctx.variable("afterInsert").get(0);
                    double timeInSeconds = (afterRead - before) / 1000d;
                    System.out.println("\twithoutRelation " + times / timeInSeconds + " ops/s");
                    ctx.continueTask();
                });
                t.readVar("user");
                t.loop("0", "" + times,
                        newTask().travelInTime("{{i}}").traverse("position").isolate(newTask().attribute("lat").attribute("long"))
                );
                t.thenDo(ctx -> {
                    long afterReadRelation = System.currentTimeMillis();
                    long before = (Long) ctx.variable("afterRead").get(0);
                    double timeInSeconds = (afterReadRelation - before) / 1000d;
                    System.out.println("\twithRelation " + times / timeInSeconds + " ops/s");
                    ctx.continueTask();
                });
                t.execute(g, null);
            }
        });
    }

}
