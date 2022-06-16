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

"""SFPDCADCOT Functions."""

import xml.etree.ElementTree as ET

from configparser import SectionProxy
from typing import Union, Set

import pytak
import sfpdcadcot  # pylint: disable=cyclic-import

__author__ = "Greg Albrecht W2GMD <oss@undef.net>"
__copyright__ = "Copyright 2022 Greg Albrecht"
__license__ = "Apache License, Version 2.0"


def create_tasks(
    config: SectionProxy, clitool: pytak.CLITool
) -> Set[pytak.Worker,]:
    """
    Creates specific coroutine task set for this application.

    Parameters
    ----------
    config : `ConfigParser`
        Configuration options & values.
    clitool : `pytak.CLITool`
        A PyTAK Worker class instance.

    Returns
    -------
    `set`
        Set of PyTAK Worker classes for this application.
    """
    return set([sfpdcadcot.CADWorker(clitool.tx_queue, config)])


def call_to_cot_xml(call: dict, config: Union[SectionProxy, None] = None) -> Union[ET.Element, None]:
    """
    Serializes a SFPD CAD calls as Cursor-On-Target XML.

    Parameters
    ----------
    craft : `dict`
        Key/Value data struct of CAD data.
    config : `configparser.SectionProxy`
        Configuration options and values.

    Returns
    -------
    `xml.etree.ElementTree.Element`
        Cursor-On-Target XML ElementTree object.
    """
    config: dict = config or {}

    lon, lat = call.intersection_point["coordinates"]
    if lat is None or lon is None:
        return None

    remarks_fields = []

    cot_stale = int(config.get("COT_STALE"))
    cot_host_id = config.get("COT_HOST_ID", pytak.DEFAULT_HOST_ID)
    cot_uid = f"SFPDCAD.{call.cad_number}"
    cot_type = "a-u-G"

    callsign = f"{call.call_type_final} {call.call_type_final_desc}"

    remarks_fields.append(callsign)
    remarks_fields.append(call.intersection_name)
    remarks_fields.append(call.police_district)
    remarks_fields.append(f"CAD: {call.cad_number}")


    point = ET.Element("point")
    point.set("lat", str(lat))
    point.set("lon", str(lon))

    point.set("ce", str("9999999.0"))
    point.set("le", str("9999999.0"))
    point.set("hae", str("9999999.0"))

    uid = ET.Element("UID")
    uid.set("Droid", str(callsign))

    contact = ET.Element("contact")
    contact.set("callsign", str(callsign))

    track = ET.Element("track")
    track.set("course", str("9999999.0"))
    track.set("speed", str("9999999.0"))

    detail = ET.Element("detail")
    detail.set("uid", cot_uid)
    detail.append(uid)
    detail.append(contact)
    detail.append(track)

    remarks = ET.Element("remarks")

    remarks_fields.append(f"{cot_host_id}")

    _remarks = " - ".join(list(filter(None, remarks_fields)))

    remarks.text = _remarks
    detail.append(remarks)

    root = ET.Element("event")
    root.set("version", "2.0")
    root.set("type", cot_type)
    root.set("uid", cot_uid)
    root.set("how", "m-g")
    # FIXME: Convert CAD timestamp to UTC, and use here:
    root.set("time", pytak.cot_time())
    root.set("start", pytak.cot_time())
    root.set("stale", pytak.cot_time(cot_stale))

    root.append(point)
    root.append(detail)

    return root


def call_to_cot(call: dict, config: Union[dict, None] = None) -> Union[bytes, None]:
    """Wrapper that returns COT as an XML string."""
    cot: Union[ET.Element, None] = call_to_cot_xml(call, config)
    return ET.tostring(cot) if cot else None


# (28, index                                                                        919
# id                                                                      17233099
# cad_number                                                             221650608
# received_datetime                                        2022-06-14T08:00:57.000
# entry_datetime                                           2022-06-14T08:04:29.000
# dispatch_datetime                                        2022-06-14T10:28:17.000
# enroute_datetime                                         2022-06-14T10:28:17.000
# onscene_datetime                                                             NaN
# close_datetime                                                               NaN
# call_type_original                                                           853
# call_type_original_desc                                      RECOVER MISSING VEH
# call_type_final                                                              853
# call_type_final_desc                                         RECOVER MISSING VEH
# priority_orginal                                                               C
# priority_final                                                                 C
# agency                                                                    Police
# disposition                                                                  NaN
# onview_flag                                                                    N
# sensitive_call                                                             False
# call_last_updated_at                                  2022-06-14 10:28:18.910000
# data_as_of                                               2022-06-16T09:43:01.750
# data_loaded_at                                        2022-06-16 09:46:26.484000
# intersection_name                                          07TH ST \ HARRISON ST
# intersection_id                                                       23929000.0
# intersection_point             {'type': 'Point', 'coordinates': [-122.406296,...
# supervisor_district                                                          6.0
# analysis_neighborhood                                            South of Market
# police_district                                                         SOUTHERN
# :@computed_region_ajp5_b2md                                                 34.0
# call_type_original_notes                                                     NaN
# call_type_final_notes                                                        NaN
# ts                                                           2022-06-14 08:00:57