import pandas as pd
import requests

from datetime import date


bcp_api_host = "lrs9glzzsf.execute-api.us-east-1.amazonaws.com"
bcp_env = "prod"

kt_game_type = 26


# TODO - let strs/dates be passed instead of just strs
def list_kt_events(st: str, et: str, limit: int = 200, only_ended: bool = True) -> pd.DataFrame:
    event_list_url = f"https://{bcp_api_host}/{bcp_env}/eventlistings"
    params = {
        "startDate": st,
        "endDate": et,
        "gameType": kt_game_type,
        "limit": limit
    }

    r = requests.get(event_list_url, params=params)

    if r.status_code != requests.codes.ok:
        print("there was a problem querying the bcp api")
        r.raise_for_status()

    es = pd.DataFrame(r.json())

    if only_ended:
        es = es[es.ended].copy()

    return es


def get_kt_event_placings(eid: str, limit: int = 500) -> pd.DataFrame:
    event_placing_url = f"https://{bcp_api_host}/{bcp_env}/players"
    params = {
        "eventId": eid,
        "inclEvent": "false",

        # TODO - can use this to have some things included, needs another "metrics" param which tells it what to include?
        "inclMetrics": "false",
        "inclArmies": "true",
        "limit": limit

        # inclTeams
    }

    r = requests.get(event_placing_url, params=params)

    if r.status_code != requests.codes.ok:
        print("there was a problem querying the bcp api")
        r.raise_for_status()

    return pd.json_normalize(r.json())


def get_kt_event_pairings(eid: str, limit: int = 500) -> pd.DataFrame:
    event_pairing_url = f"https://{bcp_api_host}/{bcp_env}/pairings"
    params = {
        "eventId": eid,
        "limit": limit
    }

    r = requests.get(event_pairing_url, params=params)

    if r.status_code != requests.codes.ok:
        print("there was a problem querying the bcp api")
        r.raise_for_status()

    return pd.json_normalize(r.json())


if __name__ == "__main__":
    evts = list_kt_events("2022-10-01", "2022-11-15", limit=10)
    eid = evts.eventObjId.iloc[0]
    eps = get_kt_event_placings(eid)
    # eps = get_kt_event_pairings(eid)

    import pdb; pdb.set_trace()
