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

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.apache.kafka.common.utils.MockTime;
import org.junit.jupiter.api.Test;

class FailureBudgeterTest {

    @Test
    public void testFailureBudgeter() {
        final MockTime time = new MockTime(0, 0, 0);
        final FailureBudgeter failureBudgeter = new FailureBudgeter(100, 1000, 5, time, 1000);
        assertNextDelayIs(failureBudgeter, 100);
        assertNextDelayIs(failureBudgeter, 100);
        assertNextDelayIs(failureBudgeter, 100);
        assertNextDelayIs(failureBudgeter, 100);
        assertNextDelayIs(failureBudgeter, 100);
        assertNextDelayIs(failureBudgeter, 100); // budget is zero
        assertNextDelayIs(failureBudgeter, 200);
        assertNextDelayIs(failureBudgeter, 400);
        assertNextDelayIs(failureBudgeter, 600);
        assertNextDelayIs(failureBudgeter, 800);
        assertNextDelayIs(failureBudgeter, 1000);
        assertNextDelayIs(failureBudgeter, 1000);
    }

    @Test
    public void testFailureBudgeterSteadyFailures() {
        final MockTime time = new MockTime(0, 0, 0);
        final FailureBudgeter failureBudgeter = new FailureBudgeter(100, 1000, 5, time, 1000);
        assertNextDelayIs(failureBudgeter, 100);
        time.setCurrentTimeMs(time.milliseconds() + 200);
        assertNextDelayIs(failureBudgeter, 100);
        time.setCurrentTimeMs(time.milliseconds() + 200);
        assertNextDelayIs(failureBudgeter, 100);
        time.setCurrentTimeMs(time.milliseconds() + 200);
        assertNextDelayIs(failureBudgeter, 100);
        time.setCurrentTimeMs(time.milliseconds() + 200);
        assertNextDelayIs(failureBudgeter, 100);
        time.setCurrentTimeMs(time.milliseconds() + 200);
        assertNextDelayIs(failureBudgeter, 100);
        time.setCurrentTimeMs(time.milliseconds() + 200);
        assertNextDelayIs(failureBudgeter, 100);
        time.setCurrentTimeMs(time.milliseconds() + 200);
        assertNextDelayIs(failureBudgeter, 100);
        time.setCurrentTimeMs(time.milliseconds() + 200);
        assertNextDelayIs(failureBudgeter, 100);
        time.setCurrentTimeMs(time.milliseconds() + 200);
        assertNextDelayIs(failureBudgeter, 100);
        time.setCurrentTimeMs(time.milliseconds() + 200);
        assertNextDelayIs(failureBudgeter, 100);
        time.setCurrentTimeMs(time.milliseconds() + 200);
        assertNextDelayIs(failureBudgeter, 100);
    }

    @Test
    public void testFailureBudgeterRateExceeded() {
        final MockTime time = new MockTime(0, 0, 0);
        final FailureBudgeter failureBudgeter = new FailureBudgeter(100, 1000, 5, time, 1000);
        assertNextDelayIs(failureBudgeter, 100);
        time.setCurrentTimeMs(time.milliseconds() + 150);
        assertNextDelayIs(failureBudgeter, 100);
        time.setCurrentTimeMs(time.milliseconds() + 150);
        assertNextDelayIs(failureBudgeter, 100);
        time.setCurrentTimeMs(time.milliseconds() + 150);
        assertNextDelayIs(failureBudgeter, 100);
        time.setCurrentTimeMs(time.milliseconds() + 150);
        assertNextDelayIs(failureBudgeter, 100);
        time.setCurrentTimeMs(time.milliseconds() + 150);
        assertNextDelayIs(failureBudgeter, 100);
        time.setCurrentTimeMs(time.milliseconds() + 150);
        assertNextDelayIs(failureBudgeter, 200);
        time.setCurrentTimeMs(time.milliseconds() + 150);
        assertNextDelayIs(failureBudgeter, 100);
        time.setCurrentTimeMs(time.milliseconds() + 150);
        assertNextDelayIs(failureBudgeter, 100);
        time.setCurrentTimeMs(time.milliseconds() + 150);
        assertNextDelayIs(failureBudgeter, 100);
        time.setCurrentTimeMs(time.milliseconds() + 150);
        assertNextDelayIs(failureBudgeter, 100);
        time.setCurrentTimeMs(time.milliseconds() + 150);
        assertNextDelayIs(failureBudgeter, 200);
        time.setCurrentTimeMs(time.milliseconds() + 150);
        assertNextDelayIs(failureBudgeter, 400);
        time.setCurrentTimeMs(time.milliseconds() + 150);
        assertNextDelayIs(failureBudgeter, 600);
        time.setCurrentTimeMs(time.milliseconds() + 150);
        assertNextDelayIs(failureBudgeter, 100);
        time.setCurrentTimeMs(time.milliseconds() + 150);
        assertNextDelayIs(failureBudgeter, 100);
        time.setCurrentTimeMs(time.milliseconds() + 150);
        assertNextDelayIs(failureBudgeter, 200);
    }

    @Test
    public void testFailureBudgeterWindowClearedAfter1000ms() {
        final MockTime time = new MockTime(0, 0, 0);
        final FailureBudgeter failureBudgeter = new FailureBudgeter(100, 1000, 5, time, 1000);
        assertNextDelayIs(failureBudgeter, 100);
        assertNextDelayIs(failureBudgeter, 100);
        assertNextDelayIs(failureBudgeter, 100);
        assertNextDelayIs(failureBudgeter, 100);
        assertNextDelayIs(failureBudgeter, 100);
        assertNextDelayIs(failureBudgeter, 100); // budget is zero
        assertNextDelayIs(failureBudgeter, 200);
        time.setCurrentTimeMs(1000);
        assertNextDelayIs(failureBudgeter, 100);
        assertNextDelayIs(failureBudgeter, 100);
        assertNextDelayIs(failureBudgeter, 100);
        assertNextDelayIs(failureBudgeter, 100);
        assertNextDelayIs(failureBudgeter, 100);
        assertNextDelayIs(failureBudgeter, 100);
        assertNextDelayIs(failureBudgeter, 200);
    }


    private static void assertNextDelayIs(final FailureBudgeter failureBudgeter, final int expected) {
        final int delay = failureBudgeter.getDelay();
        assertEquals(expected, delay);
    }

}