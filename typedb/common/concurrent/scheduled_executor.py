#
# Copyright (C) 2021 Vaticle
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
import sched
import time
from threading import Thread
from typing import Callable, List


class ScheduledExecutor:

    def __init__(self):
        self._tasks: List[ScheduledExecutor.FixedRateTask] = []

    def schedule_at_fixed_rate(self, interval: float, action: Callable, thread_name: str = None):
        task = ScheduledExecutor.FixedRateTask(interval, action, thread_name)
        self._tasks.append(task)
        task.start()

    def shutdown(self):
        for task in self._tasks:
            task.cancel()

    class FixedRateTask:

        def __init__(self, interval: float, action: Callable, thread_name: str):
            self._scheduler = sched.scheduler(time.time, time.sleep)
            self._interval = interval
            self._action = action
            self._thread_name = thread_name
            self._scheduled_activity = None
            self._cancelled = False

        def start(self):
            self._schedule_run()
            Thread(target=self._scheduler.run, name=self._thread_name, daemon=True).start()

        def _schedule_run(self):
            self._scheduled_activity = self._scheduler.enter(delay=self._interval, priority=1, action=self._on_tick)

        def _on_tick(self):
            if self._cancelled:
                return
            self._schedule_run()
            try:
                self._action()
            except Exception as e:
                print(e)

        def cancel(self):
            self._cancelled = True
            if self._scheduled_activity:
                self._scheduler.cancel(self._scheduled_activity)
