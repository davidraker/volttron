# -*- coding: utf-8 -*- {{{
# ===----------------------------------------------------------------------===
#
#                 Component of Eclipse VOLTTRON
#
# ===----------------------------------------------------------------------===
#
# Copyright 2023 Battelle Memorial Institute
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy
# of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.
#
# ===----------------------------------------------------------------------===
# }}}
import asyncio
import logging

from openleadr.client import OpenADRClient, enums
from openleadr.objects import Event
from volttron.platform import jsonapi
import abc

from .constants import (
    VEN_NAME,
    VTN_URL,
    DEBUG,
    CERT,
    KEY,
    PASSPHRASE,
    VTN_FINGERPRINT,
    SHOW_FINGERPRINT,
    CA_FILE,
    VEN_ID,
    DISABLE_SIGNATURE,
)
from openleadr.enums import OPT, REPORT_NAME, MEASUREMENTS
from datetime import timedelta, datetime, date, time, timezone
from typing import Callable
from volttron.platform.agent.utils import format_timestamp, setup_logging

setup_logging()
_log = logging.getLogger(__name__)


class OpenADRReportName(REPORT_NAME):
    def __init__(self):
        super.__init__()


class OpenADRMeasurements(MEASUREMENTS):
    def __init__(self):
        super.__init__()


class OpenADROpt(OPT):
    def __init__(self):
        super.__init__()


class OpenADREvent:
    def __init__(self, event: Event):
        self.event = event

    def get_event_signals(self):
        return self.event.get("event_signals")[0]

    def isTestEvent(self):
        return self.event["event_descriptor"]["test_event"]

    def parse_event(self) -> Event:
        """Parse event so that it properly displays on message bus.

        :param obj: The event received from a VTN
        :return: A deserialized Event that is converted into a python object
        """

        # function that gets called for objects that can’t otherwise be serialized.
        def _default_serialzer(x):
            if isinstance(x, timedelta):
                return int(x.total_seconds())
            elif isinstance(x, datetime):
                return format_timestamp(x)
            elif isinstance(x, date):
                return x.isoformat()
            elif isinstance(x, time):
                return x.isoformat()
            elif isinstance(x, timezone):
                return int(x.utcoffset().total_seconds())
            else:
                return None

        obj_string = jsonapi.dumps(self.event, default=_default_serialzer)
        return jsonapi.loads(obj_string)


class OpenADRClientInterface(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    async def run(self):
        pass

    @abc.abstractmethod
    def get_ven_name(self):
        pass

    @abc.abstractmethod
    def add_handler(self, event: OpenADREvent, function):
        pass

    @abc.abstractmethod
    def add_report(
        self,
        callback: Callable,
        report_name: OpenADRReportName,
        resource_id: str,
        measurement: OpenADRMeasurements,
    ):
        pass


class VolttronOpenADRClient(OpenADRClientInterface):
    def __init__(self, openadr_client: OpenADRClient) -> None:
        self._openadr_client = openadr_client

    @staticmethod
    def build_client(config):
        # Creates a VEN client using openleadr library
        return VolttronOpenADRClient(
            OpenADRClient(
                config.get(VEN_NAME),
                config.get(VTN_URL),
                debug=config.get(DEBUG),
                cert=config.get(CERT),
                key=config.get(KEY),
                passphrase=config.get(PASSPHRASE),
                vtn_fingerprint=config.get(VTN_FINGERPRINT),
                show_fingerprint=config.get(SHOW_FINGERPRINT, True),
                ca_file=config.get(CA_FILE),
                ven_id=config.get(VEN_ID),
                disable_signature=config.get(DISABLE_SIGNATURE),
            )
        )

    ##### Abstract methods implemented#####
    async def run(self):
        await self._openadr_client.run()

    def get_ven_name(self):
        return self._openadr_client.ven_name

    def add_handler(self, event, function):
        self._openadr_client.add_handler(event, function)

    def add_report(self, callback, resource_id, measurement=None,
                   data_collection_mode='incremental',
                   report_specifier_id=None, r_id=None,
                   report_name='TELEMETRY_USAGE',
                   reading_type=enums.READING_TYPE.DIRECT_READ,
                   report_type=enums.REPORT_TYPE.READING,
                   report_duration=None, report_dtstart=None,
                   sampling_rate=None, data_source=None,
                   scale="none", unit=None, power_ac=True, power_hertz=50, power_voltage=230,
                   market_context=None, end_device_asset_mrid=None, report_data_source=None):

        report_specifier_id, r_id = self._openadr_client.add_report(callback, resource_id, measurement, data_collection_mode,
                                               report_specifier_id, r_id, report_name, reading_type, report_type,
                                               report_duration, report_dtstart, sampling_rate, data_source, scale, unit,
                                               power_ac, power_hertz, power_voltage, market_context,
                                               end_device_asset_mrid, report_data_source)
        _log.info(f'######### REPORTS HAS: {self._openadr_client.reports}')
        if self._openadr_client.reports:
            asyncio.ensure_future(self._openadr_client.register_reports(self._openadr_client.reports))
            self._openadr_client.report_queue_task = self._openadr_client.loop.create_task(
                self._openadr_client._report_queue_worker())
        else:
            _log.info('########## FAILED TO FIND A REPORT')
        return report_specifier_id, r_id
