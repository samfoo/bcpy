import inflection
import os
import pandas as pd
import numpy as np
import requests
import json
import time

from tqdm import tqdm
from dataclasses import dataclass
from urllib.parse import urljoin
from typing import Any, Optional
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


def _kt_events_df(rj: list[dict[Any, Any]]) -> pd.DataFrame:
    es = pd.DataFrame(rj)

    # un-mongo-ify the columns
    es.columns = [inflection.underscore(c) for c in es.columns]

    es = es[[
        "event_date",
        "started",
        "ended",
        "name",
        "event_obj_id",
        "number_of_rounds",
        "total_players",
        "checked_in_players",
        "state",
        "country",
    ]]

    return es.copy()


def list_kt_events(st: str, et: str, limit: int = 200, offset: Optional[int] = None) -> pd.DataFrame:
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

    if offset is not None:
        params["offset"] = offset

    rj = bcp_request("eventlistings", params)

    return _kt_events_df(rj)


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
    ps["seed"] = ps.bracket_seed

    # Do an unbelievably dodgy sort that I *think* re-implements the BCP
    # algo close enough to correctly...
    #
    # TODO: This needs to handle pods??? Honestly not sure any KT events even
    # us pods. LGT didn't, and I'm assuming that's one of the largest.
    for pm in place_details:
        if pm.key in ps.columns:
            ps[f"sort_{pm.key}"] = ps[pm.key]
            if pm.negative:
                ps[f"sort_{pm.key}"] *= -1
        else:
            print(f"⚠️  unable to find key `{pm.key}` to sort placing for event {ps.eventId.iloc[0]}, defaulting to 0")
            ps[f"sort_{pm.key}"] = 0

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


def _kt_pairings_df(rj: list[dict[Any, Any]]) -> pd.DataFrame:
    ps = pd.json_normalize(rj)

    # un-mongo-ify the columns
    ps.columns = [inflection.underscore(c) for c in ps.columns]

    if len(ps[ps.pairing_table == "TeamPairing"]) > 0:
        print(f"⚠️  unable to retrieve pairings for event `{ps.event_id.iloc[0]}` since it uses unsupported `TeamPairing` mode")
        return None

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

        if f"{p}_game_result" not in ps.columns:
            print(f"⚠️  unable to determine winner for pairing in event `{ps.event_id.iloc[0]}` so dropping all results from the event")
            return None

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

    return _kt_pairings_df(rj)


def get_all_kt_events(st: str, et: str) -> pd.DataFrame:
    print(f"getting all kill team events between {st} and {et}...")
    page_size = 200
    offset = 0

    rs = []
    while True:
        print(f"\t ... fetch from offset {offset}")
        es = list_kt_events(st, et, limit=page_size, offset=offset if offset > 0 else None)
        rs.append(es)

        if len(es) < page_size:
            break
        else:
            offset += page_size

    all_es = pd.concat(rs, ignore_index=True)

    print(f"\t✅ got {len(all_es)} kill team events")

    return all_es


def _dump(name: str, df: pd.DataFrame, csv: bool, parquet: bool):
    print(f"dumping {name} (csv={csv}, parquet={parquet})")

    if csv:
        df.to_csv(f"{name}.csv", index=False)

    if parquet:
        df.to_parquet(f"{name}.parquet")


def dump_kt_meta_raw(st: str, et: str, csv: bool = True, parquet: bool = True):
    evts = get_all_kt_events(st, et)

    print("\nfiltering to only events that were both started and ended by organisers")
    evts_complete = evts[evts.started & evts.ended]
    print(f"\tevents completed: {len(evts_complete)}/{len(evts)}")

    _dump("events", evts_complete, csv, parquet)

    eids = evts_complete.event_obj_id.to_list()

    # print(f"\nretrieving placings for {len(eids)} events")
    # pl_rs = []
    # for eid in tqdm(eids):
    #     pl = get_kt_event_placings(eid)
    #     pl_rs.append(pl)
    #
    #     # TODO - randomise / increase / be a bit more polite to their API
    #     time.sleep(0.1)
    #
    # pls = pd.concat(pl_rs, ignore_index=True)
    # print(f"\tplacings completed: {len(pls)}")
    #
    # _dump("placings", pls, csv, parquet)

    print(f"\nretrieving pairings for {len(eids)} events")
    pr_rs = []
    for eid in tqdm(eids):
        pr = get_kt_event_pairings(eid)
        pr_rs.append(pr)

        # TODO - randomise / increase / be a bit more polite to their API
        time.sleep(0.1)

    prs = pd.concat(pr_rs, ignore_index=True)
    print(f"\tpairings completed: {len(prs)}")

    _dump("pairings", prs, csv, parquet)


if __name__ == "__main__":
    dump_kt_meta_raw("2022-01-01", "2022-11-01")
