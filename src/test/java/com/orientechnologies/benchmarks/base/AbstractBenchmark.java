package com.orientechnologies.benchmarks.base;

import com.orientechnologies.common.io.OFileUtils;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.OConstants;
import com.orientechnologies.orient.core.Orient;
import junit.framework.TestCase;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author Luca Garulli
 */
public abstract class AbstractBenchmark<T> extends TestCase {

  public class Data {
    private final long         beginTime  = System.currentTimeMillis();
    private long               endTime;
    private long               elapsed;
    private final long         totalItems;
    private final List<long[]> ramMetrics = new ArrayList<long[]>();

    public Data(final long totalItems) {
      this.totalItems = totalItems;
    }
  }

  public interface Step {
    void execute(long items);
  }

  public interface ThreadOperation<T> {
    void execute(T iDatabase, int iThreadId, long iFirst, long iLast);
  }

  private final String            name;
  private final Data              data;
  private volatile String         lastStepName;
  private final Map<String, Data> steps             = new ConcurrentHashMap<String, Data>();
  private PrintStream             out               = System.out;

  private static final int        CONCURRENCY_LEVEL = Runtime.getRuntime().availableProcessors();
  private static final int        DEF_ITEMS         = 1000000;

  protected AbstractBenchmark(final String iTestName, final long iTotalItems) {
    this.name = iTestName;
    this.data = new Data(iTotalItems);

    out.printf("\nORIENTDB BENCHMARK SUITE [OrientDB v.%s - CPUs: %d]", name, OConstants.getVersion(), Runtime.getRuntime()
        .availableProcessors());
    out.printf("\nSTARTED TEST: %s\n", name);

    Orient.instance().scheduleTask(new TimerTask() {
      @Override
      public void run() {
        recordRAM();
      }
    }, 1000, 1000);
  }

  public AbstractBenchmark(final String iTestName) {
    this(iTestName, Long.parseLong(System.getProperty("items", "" + DEF_ITEMS)));
  }

  protected abstract void runInMultiThread(final int iThreadId, final long iThreadFirst, final long iThreadLast,
      ThreadOperation<T> iThreadOperation);

  protected abstract void closeDatabase();

  protected abstract void dropDatabase();

  protected void executeMultiThreads(final int iThreads, final long iTotalItems, final ThreadOperation<T> iThreadOperation) {
    final Thread[] threads = new Thread[iThreads];

    final long itemsEachThread = iTotalItems / iThreads;
    final long itemsFirstThread = itemsEachThread + (iTotalItems % iThreads);

    long counter = 0;
    for (int i = 0; i < iThreads; ++i) {
      final int threadId = i;
      final long threadFirst = counter;
      final long threadLast = threadFirst + (threadId == 0 ? itemsFirstThread : itemsEachThread) - 1;

      threads[i] = new Thread(new Runnable() {
        @Override
        public void run() {
          runInMultiThread(threadId, threadFirst, threadLast, iThreadOperation);
        }
      });
      threads[i].start();

      counter = threadLast + 1;
    }

    for (int i = 0; i < iThreads; ++i) {
      try {
        threads[i].join();
      } catch (InterruptedException e) {
        OLogManager.instance().error(this, "Error on joining thread", e);
      }
    }
  }

  protected long end() {
    dropDatabase();

    data.endTime = System.currentTimeMillis();
    data.elapsed = data.endTime - data.beginTime;

    dump();

    return data.elapsed;
  }

  protected long step(final String iName, final Step iStep) {
    return step(iName, data.totalItems, iStep);
  }

  protected long step(final String iName, final long iTotalItems, final Step iStep) {
    beginStep(iName, iTotalItems);
    iStep.execute(iTotalItems);
    return endStep();
  }

  protected long beginStep(final String iName, final long iTotalItems) {
    if (steps.containsKey(iName))
      throw new IllegalArgumentException("Step '" + iName + "' already run");

    lastStepName = iName;

    out.printf("\n+ STARTED STEP: %s (%s)\n", iName, getRAMStatistics());

    final Data step = new Data(iTotalItems);
    steps.put(iName, step);

    recordRAM();

    return step.beginTime;
  }

  protected long endStep() {
    final Data step = steps.get(lastStepName);

    if (step == null)
      throw new IllegalArgumentException("Step '" + lastStepName + "' never begun");

    step.endTime = System.currentTimeMillis();
    step.elapsed = step.endTime - step.beginTime;

    recordRAM();

    out.printf("\n+ COMPLETED STEP: %s in %,dms (%s)\n", lastStepName, step.elapsed, getRAMStatistics());
    System.gc();
    try {
      Thread.sleep(1000);
    } catch (InterruptedException e) {
    }

    recordRAM();

    return step.elapsed;
  }

  protected String getRAMStatistics() {
    return "total=" + OFileUtils.getSizeAsString(Runtime.getRuntime().totalMemory()) + " free="
        + OFileUtils.getSizeAsString(Runtime.getRuntime().freeMemory()) + " max="
        + OFileUtils.getSizeAsString(Runtime.getRuntime().maxMemory());
  }

  protected void dump() {
    out.printf("\n+------------------------------------------------------------------------+");
    out.printf("\n| TEST COMPLETED: %-55s|", name);
    out.printf("\n+-----------------------------------+------------------------------------+");
    out.printf("\n| Elapsed (ms)....: %-,15d | Speed (item/sec): %-,16.2f |", data.elapsed,
        ((float) data.totalItems * 1000 / data.elapsed));
    out.printf("\n| CPUs............: %-15d | OrientDB v.......: %-16s |", Runtime.getRuntime().availableProcessors(),
        OConstants.getVersion());
    if (!steps.isEmpty()) {
      final List<String> ordered = new ArrayList<String>(steps.keySet());
      Collections.sort(ordered);

      for (String stepName : ordered) {
        final Data step = steps.get(stepName);

        out.printf("\n+-----------------------------------+------------------------------------+");
        out.printf("\n| + STEP: %-63s|", stepName);
        out.printf("\n|   Elapsed (ms)....: %-,13d | Items...........: %-,16d |", step.elapsed, step.totalItems);
        out.printf("\n|   Speed (item/sec): %-,13.2f |                                    |",
            ((float) step.totalItems * 1000 / step.elapsed));
        out.printf("\n|   RAM.............: %-50s |", getRAMStatistics());
      }
    }
    out.printf("\n+-----------------------------------+------------------------------------+");
  }

  protected String getURL() {
    return System.getProperty("url", "plocal:./databases/" + name);
  }

  protected int getConcurrencyLevel() {
    return Integer.parseInt(System.getProperty("concurrencyLevel", "" + CONCURRENCY_LEVEL));
  }

  protected void recordRAM() {
    if (lastStepName == null)
      return;

    final Data data = steps.get(lastStepName);
    data.ramMetrics.add(new long[] { System.currentTimeMillis(), Runtime.getRuntime().totalMemory(),
        Runtime.getRuntime().freeMemory(), Runtime.getRuntime().maxMemory() });
  }

  protected long getTotalItems() {
    return data.totalItems;
  }
}
