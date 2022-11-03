import inflection
import os
import pandas as pd
import requests
import json

from dataclasses import dataclass
from urllib.parse import urljoin
from typing import Any
from datetime import date


bcp_api_host = "lrs9glzzsf.execute-api.us-east-1.amazonaws.com"
bcp_env = "prod"

kt_game_type = 26


@dataclass
class PlacingDetails:
    key: str
    negative: bool
    name: str


def bcp_request(path: str, params: dict[str, Any] = {}) -> list[dict[Any, Any]]:
    url = urljoin(f"https://{bcp_api_host}/{bcp_env}/", path)

    r = requests.get(url, params=params)

    if r.status_code != requests.codes.ok:
        r.raise_for_status()

    return r.json()


def _kt_events_df(rj: list[dict[Any, Any]], only_ended: bool) -> pd.DataFrame:
    """
    Clean up a bunch of stuff from the raw BCP results so that it's more useful
    to KT stats analysis.
    """

    es = pd.DataFrame(rj)

    # un-mongo-ify the columns
    es.columns = [inflection.underscore(c) for c in es.columns]

    # Exclude events that are not yet ended. This should mostly be to clean up
    # garbage events, including those which haven't even started.
    if only_ended:
        es = es[es.ended]

    es = es[[
        "event_date",
        "name",
        "event_obj_id",
        "number_of_rounds",
        "total_players",
        "checked_in_players",
        "state",
        "country",
    ]]

    return es.copy()


# TODO - let strs/dates be passed instead of just strs
def list_kt_events(st: str, et: str, limit: int = 200, only_ended: bool = True) -> pd.DataFrame:
    params = {
        "startDate": st,
        "endDate": et,
        "gameType": kt_game_type,
        "limit": limit
    }

    rj = bcp_request("eventlistings", params)

    return _kt_events_df(rj, only_ended)


def get_kt_event_placing_sort_order(eid: str) -> list[PlacingDetails]:
    """
    Super annoying this is client side... Basically have to re-implement a
    bunch of sorting logic to get places right.
    """

    rj = bcp_request(f"events/{eid}", {
        "inclMetrics": "true"
    })

    pms = rj["placingMetrics"]

    rs = []
    for pm in pms:
        if pm["isOn"]:
            rs.append(PlacingDetails(
                pm["key"],
                pm["negative"],
                pm["name"]
            ))

    return rs


def _kt_placings_df(rj: list[dict[Any, Any]], excl_dropped: bool, place_details: list[PlacingDetails]) -> pd.DataFrame:
    ps = pd.json_normalize(rj)

    # Do an unbelievably dodgy sort that I *think* re-implements the BCP
    # algo correctly...
    #
    # TODO: This needs to handle pods???
    for pm in place_details:
        ps[f"sort_{pm.key}"] = ps[pm.key]
        if pm.negative:
            ps[f"sort_{pm.key}"] *= -1


    # un-mongo-ify the columns
    ps.columns = [inflection.underscore(c) for c in ps.columns]

    if excl_dropped:
        ps = ps[~ps.dropped]

    ps = ps.rename({
        "mf_swiss_points": "tournament_points",
        "wh_control_points": "secondary_points",
    })


    #####
    #
    # BCP algo for calculating podium
    # RE notes:
    # - Seems like "overall" is hardcoded to "false" in the events I've looked
    #   at. Maybe this is something I just haven't looked at the right things
    #   yet? There's a hidden UI element that can turn it on, but hell if I
    #   know what it is.
    # - pod == "podium"?
    # - metrics lets individual event owners set their scoring or something?
    #
    #####
    #
    # function sortedPlayersArray(players, metrics, overall) {
    #     var temp = players.slice();
    #     temp.sort(function (a, b) {
    #         if (overall) {
    #             if (a.overallBattlePoints > b.overallBattlePoints) {
    #                 return -1;
    #             }
    #             if (a.overallBattlePoints < b.overallBattlePoints) {
    #                 return 1;
    #             }
    #             if (a.FFGBattlePointsSoS > b.FFGBattlePointsSoS) {
    #                 return -1;
    #             }
    #             if (a.FFGBattlePointsSoS < b.FFGBattlePointsSoS) {
    #                 return 1;
    #             }
    #         } else {
    #             if (a.podNum && !b.podNum)
    #                 return -1;
    #             if (!a.podNum && b.podNum)
    #                 return 1;
    #             if (a.podNum < b.podNum)
    #                 return -1
    #             if (a.podNum > b.podNum)
    #                 return 1;
    #             for (var i = 0; i < metrics.length; i++) {
    #                 if (metrics[i].isOn) {
    #                     if (metrics[i].negative) {
    #                         if (a['pod_'+metrics[i].key] < b['pod_'+metrics[i].key]) {
    #                             return -1
    #                         }
    #                         if (a['pod_'+metrics[i].key] > b['pod_'+metrics[i].key]) {
    #                             return 1
    #                         }
    #                     } else {
    #                         if (a['pod_'+metrics[i].key] > b['pod_'+metrics[i].key]) {
    #                             return -1
    #                         }
    #                         if (a['pod_'+metrics[i].key] < b['pod_'+metrics[i].key]) {
    #                             return 1
    #                         }
    #                     }
    #                     if (!a.podNum && !b.podNum) {
    #                     if (metrics[i].negative) {
    #                         if (a[metrics[i].key] < b[metrics[i].key]) {
    #                             return -1
    #                         }
    #                         if (a[metrics[i].key] > b[metrics[i].key]) {
    #                             return 1
    #                         }
    #                     } else {
    #                         if (a[metrics[i].key] > b[metrics[i].key]) {
    #                             return -1
    #                         }
    #                         if (a[metrics[i].key] < b[metrics[i].key]) {
    #                             return 1
    #                         }
    #                     }
    #                     }
    #                 }
    #             }
    #         }
    #         if (a.bracket_seed && !b.bracket_seed)
    #             return -1;
    #         if (!a.bracket_seed && b.bracket_seed)
    #             return 1;
    #         if (a.bracket_seed < b.bracket_seed)
    #             return -1;
    #         if (a.bracket_seed > b.bracket_seed)
    #             return 1;
    #         return 0;
    #     });
    #     return temp;
    # }

    return ps


def get_kt_event_placings(eid: str, limit: int = 500, excl_dropped: bool = True) -> pd.DataFrame:
    params = {
        "eventId": eid,
        "inclEvent": "false",

        "inclMetrics": "true",
        "metrics": json.dumps(["WHControlPoints", "battlePoints", "mfSwissPoints"]),
        "inclArmies": "true",
        "limit": limit

        # inclTeams
    }

    pds = get_kt_event_placing_sort_order(eid)
    rj = bcp_request("players", params)

    return _kt_placings_df(rj, excl_dropped, pds)


def get_kt_event_pairings(eid: str, limit: int = 500) -> pd.DataFrame:
    params = {
        "eventId": eid,
        "limit": limit
    }

    rj = requests.get("pairings", params=params)

    return pd.json_normalize(rj)


if __name__ == "__main__":
    evts = list_kt_events("2022-10-01", "2022-11-15", limit=10)
    eid = "SDauAbTSeh" # evts.event_obj_id.iloc[0]
    eps = get_kt_event_placings(eid)
    # eps = get_kt_event_pairings(eid)

    import pdb; pdb.set_trace()
