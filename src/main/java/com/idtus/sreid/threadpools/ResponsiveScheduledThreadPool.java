package com.idtus.sreid.threadpools;

import com.google.common.base.Preconditions;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Delayed;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A {@link ScheduledExecutorService} that guarantees that a given task will
 * be started within one cycle of the management thread period.
 */
public class ResponsiveScheduledThreadPool implements ScheduledExecutorService {

  /**
   * The thread that manages the work queue.
   */
  private final transient Thread managementThread;

  /**
   * The executor service that will be used to run the task when
   * its time comes up.
   */
  private final transient ExecutorService delegateExecutorService;

  /**
   * The unit-less time to wait between checking to see if
   * any tasks are ready to start.
   */
  private final transient long waitTimeScalar;

  /**
   * The time unit to interpret {@link #waitTimeScalar} as.
   */
  private final transient TimeUnit waitTimeUnit;

  /**
   * The queue used to store work before it's started.
   */
  private final transient PriorityBlockingQueue<Callable<?>> workQueue;

  /**
   * The constructor.
   * @param threadFactory the factory to use to make threads in the underlying executor
   * @param waitTimeScalar the time to wait between polling the work queue
   * @param waitTimeUnit the unit of the polling time
   */
  public ResponsiveScheduledThreadPool(
      final ThreadFactory threadFactory,
      final long waitTimeScalar,
      final TimeUnit waitTimeUnit) {

    Preconditions.checkNotNull(waitTimeUnit);

    this.waitTimeScalar = waitTimeScalar;
    this.waitTimeUnit = waitTimeUnit;

    this.delegateExecutorService = Executors.newCachedThreadPool(threadFactory);

    this.workQueue = new PriorityBlockingQueue<>();

    this.managementThread = new Thread(
        new ManagementRunnable(
            TimeUnit.MILLISECONDS.convert(
                Math.abs(waitTimeScalar),
                waitTimeUnit)));
    this.managementThread.setPriority(Thread.NORM_PRIORITY);
    this.managementThread.setName(this.getClass().getName() + " Management Thread");
    this.managementThread.setDaemon(true);
    this.managementThread.start();

  }


  public ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit) {
    return null;
  }

  public <V> ScheduledFuture<V> schedule(Callable<V> callable, long delay, TimeUnit unit) {
    return null;
  }

  public ScheduledFuture<?> scheduleAtFixedRate(Runnable command, long initialDelay, long period, TimeUnit unit) {
    return null;
  }

  public ScheduledFuture<?> scheduleWithFixedDelay(Runnable command, long initialDelay, long delay, TimeUnit unit) {
    return null;
  }

  public void shutdown() {

  }

  public List<Runnable> shutdownNow() {
    return null;
  }

  public boolean isShutdown() {
    return false;
  }

  public boolean isTerminated() {
    return false;
  }

  public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
    return false;
  }

  public <T> Future<T> submit(Callable<T> task) {
    return null;
  }

  public <T> Future<T> submit(Runnable task, T result) {
    return null;
  }

  public Future<?> submit(Runnable task) {
    return null;
  }

  public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks) throws InterruptedException {
    return null;
  }

  public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit) throws InterruptedException {
    return null;
  }

  public <T> T invokeAny(Collection<? extends Callable<T>> tasks) throws InterruptedException, ExecutionException {
    return null;
  }

  public <T> T invokeAny(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
    return null;
  }

  public void execute(Runnable command) {

  }

  private final class ManagementRunnable implements Runnable {
    private final long waitDurationMillis;

    public ManagementRunnable(final long waitDurationMillis) {
      this.waitDurationMillis = Math.abs(waitDurationMillis);
    }

    @Override
    public void run() {


    }
  }

  private final class ScheduleFutureImpl<V> implements ScheduledFuture<V> {

    @Override
    public long getDelay(TimeUnit unit) {
      return 0;
    }

    @Override
    public int compareTo(Delayed o) {
      return 0;
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
      return false;
    }

    @Override
    public boolean isCancelled() {
      return false;
    }

    @Override
    public boolean isDone() {
      return false;
    }

    @Override
    public V get() throws InterruptedException, ExecutionException {
      return null;
    }

    @Override
    public V get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
      return null;
    }
  }
}
