/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.common.network;

import org.apache.kafka.common.utils.Time;

import java.util.concurrent.atomic.AtomicLong;

/*
 * The FailureBudgeter is intended to increase the delay-on-failure if we get
 * a burst of failures over a short window. The intent is to protect some
 * resource like a third-party authentication server by backing off across all
 * connections.
 *
 * When the budgeter encounters a failure it will either start a new window or
 * extend the current windows expiry. It then tracks how many failures it has
 * encountered in this window. Then when calculating the next delay it can determine
 * if the failure budget has been exceeded and start to increase the delay to pay
 * off the deficit up to some configurable maximum delay.
 */
public class FailureBudgeter {

    private final int failedAuthenticationDelayMs;

    private final int failedAuthenticationDelayMaximumMs;

    private final int failedAuthenticationMillisPerFailureBudget;

    private final Time time;

    private final AtomicLong failureWindowStart = new AtomicLong(0L);

    private final AtomicLong failureWindowExpiry = new AtomicLong(0L);

    private final AtomicLong failureCount = new AtomicLong(0L);

    private final int windowMillis;

    public FailureBudgeter(final int failedAuthenticationDelayMs,
                           final int failedAuthenticationDelayMaximumMs,
                           final int failedAuthenticationDelaysPerSecondTarget,
                           final Time time,
                           final int windowMillis) {
        this.failedAuthenticationDelayMs = failedAuthenticationDelayMs;
        this.failedAuthenticationDelayMaximumMs = failedAuthenticationDelayMaximumMs;
        this.failedAuthenticationMillisPerFailureBudget = 1000 / failedAuthenticationDelaysPerSecondTarget;
        this.time = time;
        this.windowMillis = windowMillis;
    }

    int getDelay() {
        final long currentMillis = time.milliseconds();
        final long newExpiry = currentMillis + windowMillis;
        final long expiry = failureWindowExpiry.get();
        if (expiry <= currentMillis) {
            return handleNewWindow(currentMillis, newExpiry);
        } else {
            return handleExistingWindow(currentMillis, newExpiry);
        }
    }

    private int handleExistingWindow(final long currentMillis, final long newExpiry) {
        long failures = failureCount.get();
        long start = failureWindowStart.get();
        final int nextDelay = nextDelay(currentMillis, failures, start);
        failureCount.incrementAndGet();
        setIfGreater(failureWindowExpiry, newExpiry);
        return nextDelay;
    }

    private int handleNewWindow(final long currentMillis, final long newExpiry) {
        final int nextDelay = nextDelay(currentMillis, 0, currentMillis);
        failureCount.set(1); // parallel writes might lose some failures, doesn't need to be that accurate
        setIfGreater(failureWindowExpiry, newExpiry);
        setIfGreater(failureWindowStart, currentMillis);
        return nextDelay;
    }

    private void setIfGreater(final AtomicLong atomicLong, final long newValue) {
        atomicLong.updateAndGet(operand -> Math.max(operand, newValue));
    }

    private int nextDelay(final long currentMillis, final long failures, final long start) {
        final long budgetSeconds = ((currentMillis - start) / 1000) + 1; // rounds up, so at t=500ms we have 1 second of budget
        long budgetForWindowMs = Math.max(1, budgetSeconds) * 1000;
        long spentForWindow = failedAuthenticationMillisPerFailureBudget * failures;
        long availableBudget = budgetForWindowMs - spentForWindow;
        if (availableBudget <= 0) {
            return (int) Math.max(failedAuthenticationDelayMs, Math.min(Math.abs(availableBudget), failedAuthenticationDelayMaximumMs));
        } else {
            return failedAuthenticationDelayMs;
        }
    }
}
