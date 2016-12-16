package org.mwg.benchmark;

import java.lang.reflect.InvocationTargetException;

public class Runner {

    public static void main(String[] args) throws NoSuchMethodException, IllegalAccessException, InvocationTargetException {

        System.out.println("=== Heap ===");
        run(NoopCallback.class, false);
        run(TrampolineCallback.class, false);
        run(TrampolineTask.class, false);
        System.out.println("=== OffHeap ===");
        run(NoopCallback.class, true);
        run(TrampolineCallback.class, true);
        run(TrampolineTask.class, true);

    }

    private static void clean() {
        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        for (int i = 0; i < 3; i++) {
            System.gc();
        }
        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private static void run(Class main, boolean offHeap) throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        clean();
        System.out.println("Bench:"+main.getSimpleName());
        String[] params = new String[1];
        params[0] = offHeap ? "offheap" : "heap";
        main.getMethod("main", String[].class).invoke(main, (Object) params);
    }

}
