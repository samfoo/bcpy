import inflection
import os
import pandas as pd
import numpy as np
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
    """
    Wrap requests to the BCP JSON API. Raises an error if there's a non-200
    result.
    """

    url = urljoin(f"https://{bcp_api_host}/{bcp_env}/", path)

    r = requests.get(url, params=params)

    if r.status_code != requests.codes.ok:
        r.raise_for_status()

    return r.json()


def _kt_events_df(rj: list[dict[Any, Any]], only_ended: bool) -> pd.DataFrame:
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


def list_kt_events(st: str, et: str, limit: int = 200, only_ended: bool = True) -> pd.DataFrame:
    """
    Return a `pd.DataFrame` of Kill Team events between the given times with
    columns relevant for stats calculations.
    """

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
    BCP organisers can configure their events with different ways of sorting to
    determine the podium, with ties ultimately also being broken by a timestamp
    (I'm guessing either account creation time or time of registering for the
    event or something).

    Irritatingly, BCP then decides to implement the actual sorting on the
    client. So to determine the actual placings, a client has to re-implement
    the sort order.

    The sort order appears to have a number of complications (pods, complete,
    normal - see re_notes.txt). Rather than try to implement all of that, this
    does what is the most likely.

    TODO: At some point in the future, probably need to support the other
    features if found out they are used by KT events.
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

    # So we have a way of distinguishing by event.
    ps["event_obj_id"] = eid
    ps["seed"] = ps.bracket_seed

    # Do an unbelievably dodgy sort that I *think* re-implements the BCP
    # algo close enough to correctly...
    #
    # TODO: This needs to handle pods??? Honestly not sure any KT events even
    # us pods. LGT didn't, and I'm assuming that's one of the largest.
    for pm in place_details:
        ps[f"sort_{pm.key}"] = ps[pm.key]
        if pm.negative:
            ps[f"sort_{pm.key}"] *= -1

    if excl_dropped:
        ps = ps[~ps.dropped]

    # I think this is close enough for now, assuming no pods or anything.
    ps = ps.sort_values([f"sort_{pm.key}" for pm in place_details], ascending=False)
    ps["placing"] = np.arange(len(ps))
    ps = ps.rename(columns={"index": "placing"})
    ps["placing"] += 1


    # un-mongo-ify the columns
    ps.columns = [inflection.underscore(c) for c in ps.columns]

    ps = ps.rename(columns={
        "army.name": "army_name"
    })

    ps = ps[[
        "event_id",
        "user_id",
        "placing",
        "army_id",
        "army_name",

        # TODO - lots of things that could be added here, including primaries,
        # secondaries, etc, but I think most of that is more relevant and
        # useful in the pairings dataset.
    ]]

    return ps.copy()


def get_kt_event_placings(eid: str, limit: int = 500, excl_dropped: bool = True) -> pd.DataFrame:
    """
    Return a `pd.DataFrame` of the placings for an event.
    """

    pds = get_kt_event_placing_sort_order(eid)

    params = {
        "eventId": eid,
        "inclEvent": "false",

        "inclMetrics": "true",
        "metrics": json.dumps([pm.key for pm in pds]),
        "inclArmies": "true",
        "limit": limit

        # inclTeams
    }

    rj = bcp_request("players", params)

    return _kt_placings_df(rj, excl_dropped, pds)


def _kt_placings_df(rj: list[dict[Any, Any]]) -> pd.DataFrame:
    ps = pd.json_normalize(rj)

    # un-mongo-ify the columns
    ps.columns = [inflection.underscore(c) for c in ps.columns]

    for p in ["player1", "player2"]:
        ps = ps.rename(columns={
            f"{p}.user_id": f"{p}_user_id",
            f"{p}.army": f"{p}_army",

            f"{p}.game.wh_control_points": f"{p}_game_primary_points",
            f"{p}.game.game_number": f"{p}_game_number",
            f"{p}.game.game_points": f"{p}_game_points",
            f"{p}.game.game_result": f"{p}_game_result",
            f"{p}.game.margin_of_victory": f"{p}_game_margin_of_victory"
        })

        ps[f"{p}_game_result_cat"] = ps[f"{p}_game_result"].map({
            0: "loss",
            1: "tie",
            2: "win",
        })

    player_stats = [
        "user_id",
        "army",
        "game_primary_points",
        "game_number",
        "game_points",
        "game_result",
        "game_result_cat",
        "game_margin_of_victory",
    ]

    ps = ps[[
            "event_id",
            "round",
        ] +
        [f"player1_{s}" for s in player_stats] +
        [f"player2_{s}" for s in player_stats]
    ]

    return ps.copy()


def get_kt_event_pairings(eid: str, limit: int = 500) -> pd.DataFrame:
    params = {
        "eventId": eid,
        "limit": limit
    }

    rj = bcp_request("pairings", params=params)

    return _kt_placings_df(rj)


if __name__ == "__main__":
    evts = list_kt_events("2022-10-01", "2022-11-15", limit=10)
    eid = "SDauAbTSeh" # evts.event_obj_id.iloc[0]
    # eps = get_kt_event_placings(eid)
    eps = get_kt_event_pairings(eid)

    import pdb; pdb.set_trace()
