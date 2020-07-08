package com.seanreid.threadpools;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.concurrent.Callable;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.junit.jupiter.api.Test;

class ResponsiveScheduledThreadPoolTest {

  @Test
  public void testSchedule() throws Exception {
    final ResponsiveScheduledThreadPool pool =
        new ResponsiveScheduledThreadPool(
            Thread::new,
            1,
            TimeUnit.NANOSECONDS,
            "Test pool");

    final Callable<Integer> callable = () -> 5;

    final ScheduledFuture<Integer> future =
        pool.schedule(callable, 5, TimeUnit.SECONDS);

    assertThrows(
        TimeoutException.class,
        () -> future.get(1, TimeUnit.MILLISECONDS),
        "Future should not be resolvable after 1ms");

    Thread.sleep(5000);

    final Integer result = future.get(1, TimeUnit.MILLISECONDS);
    assertEquals(5, result, "Result should be 5");

  }

}