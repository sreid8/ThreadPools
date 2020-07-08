package com.seanreid.threadpools;

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
import javax.annotation.Nonnull;
import org.apache.commons.lang3.mutable.MutableObject;

/**
 * A {@link ScheduledExecutorService} that guarantees that a given task will
 * be started within one cycle of the management thread period.
 */
public final class ResponsiveScheduledThreadPool implements ScheduledExecutorService {

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
   * The queue used to store work before it's started.
   */
  private final transient PriorityBlockingQueue<ScheduledFutureImpl<?>> workQueue;

  /**
   * The constructor.
   * @param threadFactory the factory to use to make threads in the underlying executor
   * @param waitTimeScalar the time to wait between polling the work queue.
   *                       If this value is less than 0,
   *                       the absolute value of the provided value is used.
   * @param waitTimeUnit the unit of the polling time
   */
  public ResponsiveScheduledThreadPool(
      final ThreadFactory threadFactory,
      final long waitTimeScalar,
      final TimeUnit waitTimeUnit,
      final String poolName) {

    Preconditions.checkNotNull(waitTimeUnit);
    if (waitTimeScalar <= 0) {
      throw new IllegalArgumentException("Wait time cannot be 0 or less.");
    }

    this.delegateExecutorService = Executors.newCachedThreadPool(threadFactory);

    this.workQueue = new PriorityBlockingQueue<>();

    this.managementThread = new Thread(
        new ManagementRunnable(
            TimeUnit.NANOSECONDS.convert(
                Math.abs(waitTimeScalar),
                waitTimeUnit)));
    this.managementThread.setPriority(Thread.MAX_PRIORITY);
    this.managementThread.setName(this.getClass().getName() + " " + poolName + " Management Thread");
    this.managementThread.setDaemon(true);
    this.managementThread.start();

  }

  @Override
  @Nonnull
  public ScheduledFuture<?> schedule(
      final @Nonnull Runnable runnable,
      final long delay,
      final @Nonnull TimeUnit unit) {

    final Callable<?> callable = Executors.callable(runnable);
    return schedule(callable, delay, unit);
  }

  @Override
  @Nonnull
  public <V> ScheduledFuture<V> schedule(
      final @Nonnull Callable<V> callable,
      final long delay,
      final @Nonnull TimeUnit unit) {

    final ScheduledFutureImpl<V> future = new ScheduledFutureImpl<>(delay, unit, callable);
    workQueue.add(future);
    return future;
  }

  @Override
  @Nonnull
  public ScheduledFuture<?> scheduleAtFixedRate(
      final @Nonnull Runnable runnable,
      final long initialDelay,
      final long period,
      final @Nonnull TimeUnit unit) {


    final Callable<?> callable =
        new FixedRateCallable(
            runnable,
            period,
            unit);

    ScheduledFutureImpl<?> future =
        new ScheduledFutureImpl<>(
            initialDelay,
            unit,
            callable);
    workQueue.add(future);
    return future;
  }

  @Override
  @Nonnull
  public ScheduledFuture<?> scheduleWithFixedDelay(
      final @Nonnull Runnable runnable,
      final long initialDelay,
      final long delay,
      final @Nonnull TimeUnit unit) {

    final Callable<?> callable =
        new FixedDelayCallable(
            runnable,
            delay,
            unit);

    ScheduledFutureImpl<?> future =
        new ScheduledFutureImpl<>(
            initialDelay,
            unit,
            callable);

    workQueue.add(future);
    return future;
  }

  @Override
  public void shutdown() {
    this.managementThread.interrupt();
    this.delegateExecutorService.shutdown();
  }

  @Override
  @Nonnull
  public List<Runnable> shutdownNow() {
    this.managementThread.interrupt();
    return this.delegateExecutorService.shutdownNow();
  }

  @Override
  public boolean isShutdown() {
    return
        (this.managementThread.isInterrupted()
            || !this.managementThread.isAlive())
        && this.delegateExecutorService.isShutdown();
  }

  @Override
  public boolean isTerminated() {
    return this.delegateExecutorService.isTerminated();
  }

  @Override
  public boolean awaitTermination(
      final long timeout,
      final @Nonnull TimeUnit unit)
      throws InterruptedException {

    // don't schedule any more tasks
    this.managementThread.interrupt();
    return this.delegateExecutorService.awaitTermination(timeout, unit);
  }

  @Override
  @Nonnull
  public <T> Future<T> submit(
      final @Nonnull Callable<T> task) {

    return this.delegateExecutorService.submit(task);
  }

  @Override
  @Nonnull
  public <T> Future<T> submit(
      final @Nonnull Runnable task,
      final T result) {

    return this.delegateExecutorService.submit(task, result);
  }

  @Override
  @Nonnull
  public Future<?> submit(
      final @Nonnull Runnable task) {

    return this.delegateExecutorService.submit(task);
  }

  @Override
  @Nonnull
  public <T> List<Future<T>> invokeAll(
      @Nonnull final Collection<? extends Callable<T>> tasks)
      throws InterruptedException {

    return this.delegateExecutorService.invokeAll(tasks);
  }

  @Override
  @Nonnull
  public <T> List<Future<T>> invokeAll(
      final @Nonnull Collection<? extends Callable<T>> tasks,
      final long timeout,
      final @Nonnull TimeUnit unit)
      throws InterruptedException {

    return this.delegateExecutorService.invokeAll(tasks, timeout, unit);
  }

  @Override
  @Nonnull
  public <T> T invokeAny(
      final @Nonnull Collection<? extends Callable<T>> tasks)
      throws InterruptedException, ExecutionException {

    return this.delegateExecutorService.invokeAny(tasks);
  }

  @Override
  public <T> T invokeAny(
      final @Nonnull Collection<? extends Callable<T>> tasks,
      final long timeout,
      final @Nonnull TimeUnit unit)
      throws InterruptedException, ExecutionException, TimeoutException {

    return this.delegateExecutorService.invokeAny(tasks, timeout, unit);
  }

  @Override
  public void execute(
      final @Nonnull Runnable command) {

    this.delegateExecutorService.execute(command);
  }

  /**
   * The runnable that manages transitioning scheduled tasks into
   * executing tasks on time.
   */
  private final class ManagementRunnable implements Runnable {

    /**
     * The amount of time to sleep between attempts
     * to begin tasks who's scheduled time has come, in milliseconds.
     */
    private final transient long waitDurationMillis;

    /**
     * Any additional time to sleep after the sleep millis has passed, in nanos.
     */
    private final transient int waitDurationNanos;

    /**
     * Constructor.
     * @param waitDurationNanos the time in nano seconds to wait between
     *                          attempts to start scheduled tasks.
     */
    public ManagementRunnable(final long waitDurationNanos) {
      final long durationNanos = Math.abs(waitDurationNanos);
      this.waitDurationMillis =
          TimeUnit.MILLISECONDS.convert(
              durationNanos,
              TimeUnit.NANOSECONDS);
      this.waitDurationNanos = (int)
          (durationNanos - TimeUnit.NANOSECONDS.convert(
              this.waitDurationMillis,
              TimeUnit.MILLISECONDS));
    }

    @Override
    @SuppressWarnings("unchecked")
    public void run() {
      while (!Thread.currentThread().isInterrupted()) {
        while (workQueue.peek() != null
            && System.nanoTime() >= workQueue.peek().getExecutionTime()) {
          final ScheduledFutureImpl<?> scheduledFuture = workQueue.poll();
          if (scheduledFuture != null) {
            synchronized (scheduledFuture) {
              final Future future =
                  delegateExecutorService.submit(scheduledFuture.getCallable());
              scheduledFuture.setDelegateFuture(future);
            }
          }
        }
        try {
          Thread.sleep(
              this.waitDurationMillis,
              this.waitDurationNanos);
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }
    }
  }

  /**
   * An implementation of {@link ScheduledFuture}.
   * @param <V> the expected return type
   */
  private final class ScheduledFutureImpl<V> implements ScheduledFuture<V> {

    /**
     * The object that stores the delegate future.
     */
    private final transient MutableObject<Future<V>> delegateFuture;

    /**
     * Whether or not this future has been cancelled.
     */
    private volatile transient boolean cancelled;

    /**
     * The number of nanoseconds to delay before executing.
     */
    private final transient long delayNanos;

    /**
     * The nanosecond time at which to begin execution.
     */
    private final transient long executionTime;

    /**
     * The callable to run when the delay has expired.
     */
    private final transient Callable<V> callable;


    public ScheduledFutureImpl(
        final long delay,
        final TimeUnit unit,
        final Callable<V> callable) {

      this.delegateFuture = new MutableObject<>(null);
      this.cancelled = false;
      this.delayNanos = TimeUnit.NANOSECONDS.convert(delay, unit);
      this.executionTime = System.nanoTime() + this.delayNanos;
      this.callable = callable;
    }

    /**
     * Sets the true future once the task has begun actually executing.
     * @param future the true future
     */
    private void setDelegateFuture(final Future<V> future) {
      synchronized (this) {
        synchronized (this.delegateFuture) {
          this.delegateFuture.setValue(future);
        }
        this.notifyAll();
      }
    }

    /**
     * Gets the time at which to execute this task.
     * @return the nanosecond time at which to execute this task
     */
    private long getExecutionTime() {
      return this.executionTime;
    }

    /**
     * Returns the {@link Callable} to run when the scheduled time comes.
     * @return the callable
     */
    private Callable<V> getCallable() {
      return this.callable;
    }

    @Override
    public long getDelay(TimeUnit unit) {
      return unit.convert(delayNanos, TimeUnit.NANOSECONDS);
    }

    @Override
    public int compareTo(Delayed o) {
      if (o instanceof ScheduledFutureImpl<?>) {
        final long difference =
            this.executionTime
                - ((ScheduledFutureImpl<?>) o).getExecutionTime();
        if (difference < 0) {
          return -1;
        }
        if (difference > 0) {
          return 1;
        }
        return 0;
      }
      return (int)(this.delayNanos - o.getDelay(TimeUnit.NANOSECONDS));
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
      final boolean delegateFuturePresent;
      synchronized (this.delegateFuture) {
        delegateFuturePresent = this.delegateFuture.getValue() != null;
      }
      if (!delegateFuturePresent) {
        workQueue.remove(this);
        this.cancelled = true;
      } else {
        synchronized (this.delegateFuture) {
          this.cancelled = this.delegateFuture.getValue().cancel(mayInterruptIfRunning);
        }
      }
      return this.cancelled;
    }

    @Override
    public boolean isCancelled() {
      final boolean delegateFuturePresent;
      synchronized (this.delegateFuture) {
        delegateFuturePresent = this.delegateFuture.getValue() != null;
      }
      if (!delegateFuturePresent) {
        return this.cancelled;
      }
      synchronized (this.delegateFuture) {
        return this.delegateFuture.getValue().isCancelled();
      }
    }

    @Override
    public boolean isDone() {
      final boolean delegateFuturePresent;
      synchronized (this.delegateFuture) {
        delegateFuturePresent = this.delegateFuture.getValue() != null;
      }
      if (!delegateFuturePresent) {
        return false;
      }
      synchronized (this.delegateFuture) {
        return this.delegateFuture.getValue().isDone();
      }
    }

    @Override
    public V get() throws InterruptedException, ExecutionException {
      final boolean delegateFuturePresent;
      synchronized (this.delegateFuture) {
        delegateFuturePresent = this.delegateFuture.getValue() != null;
      }
      if (!delegateFuturePresent) {
        synchronized (this) {
          this.wait();
        }
      }
      final Future<V> delegate;
      synchronized (this.delegateFuture) {
        delegate = this.delegateFuture.getValue();
      }
      return delegate.get();
    }

    @Override
    public V get(
        final long timeout,
        final @Nonnull TimeUnit unit)
        throws InterruptedException, ExecutionException, TimeoutException {

      final boolean delegateFuturePresent;
      synchronized (this.delegateFuture) {
        delegateFuturePresent = this.delegateFuture.getValue() != null;
      }

      final long start = System.nanoTime();
      long remaining = 0;
      if (!delegateFuturePresent) {
        synchronized (this) {
          this.wait(TimeUnit.MILLISECONDS.convert(timeout, unit));
        }
        final long elapsed =
            System.nanoTime() - start;

        remaining = TimeUnit.NANOSECONDS.convert(timeout, unit) - elapsed;
        if (elapsed >= TimeUnit.NANOSECONDS.convert(timeout, unit) ) {
          throw new TimeoutException("Result unavailable before timeout");
        }
      }

      final Future<V> delegate;
      synchronized (this.delegateFuture) {
        delegate = this.delegateFuture.getValue();
      }
      return delegate.get(remaining, TimeUnit.NANOSECONDS);
    }
  }

  /**
   * A callable that reschedules itself on a periodic.
   */
  private final class FixedRateCallable implements Callable<Void> {

    /**
     * The runnable to run.
     */
    private final transient Runnable runnable;

    /**
     * The period on which to reschedule self, in nanoseconds.
     */
    private final transient long periodNanos;

    /**
     * Constructor.
     * @param runnable the runnable
     * @param period the period to reschedule self
     * @param unit the unit of te period
     */
    public FixedRateCallable(
        final @Nonnull Runnable runnable,
        final long period,
        final @Nonnull TimeUnit unit) {

      this.runnable = runnable;
      this.periodNanos = TimeUnit.NANOSECONDS.convert(period, unit);
    }

    @Override
    public Void call() {
      final FixedRateCallable newSelf =
          new FixedRateCallable(
              this.runnable,
              this.periodNanos,
              TimeUnit.NANOSECONDS);

      final ScheduledFuture<?> next =
          schedule(
              newSelf,
              this.periodNanos,
              TimeUnit.NANOSECONDS);

      try {
        this.runnable.run();
      } catch (final Throwable e) {
        // cancel future iterations if an exception occurs
        next.cancel(/* cancel if it's already running */ true);
        throw e;
      }
      return null;
    }
  }

  /**
   * A callable that reschedules itself to run after the previous iteration of
   * itself has run.
   */
  private final class FixedDelayCallable implements Callable<Void> {

    /**
     * The runnable to run.
     */
    private final transient Runnable runnable;

    /**
     * The delay before the task will run again, in nanoseconds.
     */
    private final transient long delay;

    public FixedDelayCallable(
        final @Nonnull Runnable runnable,
        final long delay,
        final @Nonnull TimeUnit unit) {

      this.runnable = runnable;
      this.delay = TimeUnit.NANOSECONDS.convert(delay, unit);
    }

    @Override
    public Void call() {

      try {
        this.runnable.run();
      } catch (final Throwable e) {
        // don't schedule again if anything went wrong.
        return null;
      }

      final FixedDelayCallable newSelf =
          new FixedDelayCallable(
              this.runnable,
              this.delay,
              TimeUnit.NANOSECONDS);

      final ScheduledFutureImpl<?> future =
          new ScheduledFutureImpl<>(
              this.delay,
              TimeUnit.NANOSECONDS,
              newSelf);

      workQueue.add(future);

      return null;
    }
  }
}
