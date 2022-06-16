#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright 2022 Greg Albrecht <oss@undef.net>
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# Author:: Greg Albrecht W2GMD <oss@undef.net>
#

"""SFPDCADCOT Class Definitions."""

import asyncio

from typing import Union

import pandas as pd

import pytak
import sfpdcadcot


__author__ = "Greg Albrecht W2GMD <oss@undef.net>"
__copyright__ = "Copyright 2022 Greg Albrecht"
__license__ = "Apache License, Version 2.0"


class CADWorker(pytak.QueueWorker):

    """Reads CAD Data, renders to COT, and puts on Queue."""
    async def handle_data(self, data: list) -> None:
        """
        Transforms Data to COT and puts it onto TX queue.
        """
        calls = data.dropna(subset='intersection_point')
        calls['ts'] = pd.to_datetime(calls['received_datetime'])
        calls = calls[calls.ts.dt.minute < 15][calls.close_datetime.isnull()].sort_values(by='received_datetime', ascending=False)

        for _, call in calls.reset_index().iterrows():
            event: Union[str, None] = sfpdcadcot.call_to_cot(call, config=self.config)

            if not event:
                self._logger.debug("Empty COT")
                continue

            await self.put_queue(event)

    async def get_cad_feed(self, url: str) -> None:
        """
        Gets CAD Data.
        """
        data = pd.read_json(url)
        await self.handle_data(data)

    async def run(self, number_of_iterations=-1) -> None:
        """Runs this Thread, Reads from Pollers."""
        self._logger.info("Running %s", self.__class__)

        url: str = self.config.get("CAD_URL", sfpdcadcot.DEFAULT_CAD_URL)
        poll_interval: str = self.config.get(
            "POLL_INTERVAL", sfpdcadcot.DEFAULT_POLL_INTERVAL
        )

        while 1:
            self._logger.info("%s polling every %ss: %s", self.__class__, poll_interval, url)
            await self.get_cad_feed(url)
            await asyncio.sleep(int(poll_interval))
