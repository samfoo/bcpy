import os
import pandas as pd
import requests

from urllib.parse import urljoin
from typing import Any
from datetime import date


bcp_api_host = "lrs9glzzsf.execute-api.us-east-1.amazonaws.com"
bcp_env = "prod"

kt_game_type = 26


def bcp_request(path: str, params: dict[str, Any]) -> list[dict[Any, Any]]:
    url = urljoin(f"https://{bcp_api_host}/{bcp_env}/", path)

    r = requests.get(url, params=params)

    if r.status_code != requests.codes.ok:
        r.raise_for_status()

    return r.json()


# TODO - let strs/dates be passed instead of just strs
def list_kt_events(st: str, et: str, limit: int = 200, only_ended: bool = True) -> pd.DataFrame:
    params = {
        "startDate": st,
        "endDate": et,
        "gameType": kt_game_type,
        "limit": limit
    }

    rj = bcp_request("eventlistings", params)

    es = pd.DataFrame(rj)

    if only_ended:
        es = es[es.ended].copy()

    return es


def get_kt_event_placings(eid: str, limit: int = 500) -> pd.DataFrame:
    params = {
        "eventId": eid,
        "inclEvent": "false",

        # TODO - can use this to have some things included, needs another
        # "metrics" param which tells it what to include?
        "inclMetrics": "false",
        "inclArmies": "true",
        "limit": limit

        # inclTeams
    }

    rj = bcp_request("players", params)

    return pd.json_normalize(rj)


def get_kt_event_pairings(eid: str, limit: int = 500) -> pd.DataFrame:
    params = {
        "eventId": eid,
        "limit": limit
    }

    rj = requests.get("pairings", params=params)

    return pd.json_normalize(rj)


if __name__ == "__main__":
    evts = list_kt_events("2022-10-01", "2022-11-15", limit=10)
    eid = evts.eventObjId.iloc[0]
    eps = get_kt_event_placings(eid)
    # eps = get_kt_event_pairings(eid)

    import pdb; pdb.set_trace()
