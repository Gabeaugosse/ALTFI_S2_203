import time
import requests
import pandas as pd
import json
import os
from datetime import datetime

# Disable insecure request warnings
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

PARQUET_FILE = 'live_quotes_tennis.parquet'

# Set to True to capture LIVE matches, False for pre-match only
CAPTURE_LIVE     = True
CAPTURE_PREMATCH = True

# Refresh intervals
LIVE_INTERVAL_SECONDS     = 1   # how often live quotes are fetched/saved
PREMATCH_INTERVAL_SECONDS = 30  # how often pre-match quotes are fetched/saved

# ──────────────────────────────────────────────────────────────────────────────
# Betclic
# ──────────────────────────────────────────────────────────────────────────────

def get_betclic_quotes():
    """
    Scrape tennis odds from Betclic.

    Betclic renders its page via Angular SSR and embeds all match / odds data
    inside a <script id="ng-state" type="application/json"> tag.

    The JSON contains gRPC payload objects keyed like "grpc:<hash>".  Each
    payload that holds match data has a "matches" array.  Every match object
    has the structure:

        {
          "matchId": "...",
          "name": "Player A - Player B",
          "isLive": true | false,
          "contestants": [{"name": "Player A"}, {"name": "Player B"}],
          "markets": [
            {
              "mainSelections": [
                {"name": "Player A", "odds": 1.85},
                {"name": "Player B", "odds": 1.95}
              ]
            }
          ]
        }
    """
    url = "https://www.betclic.fr/tennis-s2"
    headers = {
        'User-Agent': (
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
            'AppleWebKit/537.36 (KHTML, like Gecko) '
            'Chrome/122.0.0.0 Safari/537.36'
        ),
        'Accept-Language': 'fr-FR,fr;q=0.9,en;q=0.8',
    }
    quotes = []

    try:
        response = requests.get(url, headers=headers, timeout=15, verify=False)
        response.raise_for_status()

        # ── 1. Extract the ng-state JSON blob ──────────────────────────────
        # The tag looks like:
        #   <script id="ng-state" type="application/json">{ ... }</script>
        html = response.text

        start_marker = '<script id="ng-state" type="application/json">'
        end_marker   = '</script>'
        start_idx = html.find(start_marker)

        if start_idx == -1:
            print("[Betclic] Could not find ng-state JSON block in page HTML.")
            return quotes

        start_idx += len(start_marker)
        end_idx = html.find(end_marker, start_idx)
        if end_idx == -1:
            print("[Betclic] Could not find closing </script> for ng-state block.")
            return quotes

        ng_state_raw = html[start_idx:end_idx]

        try:
            ng_state = json.loads(ng_state_raw)
        except json.JSONDecodeError as e:
            print(f"[Betclic] Failed to parse ng-state JSON: {e}")
            return quotes

        # ── 2. Walk every gRPC payload looking for match arrays ─────────────
        # Betclic SSR embeds gRPC responses under keys named "grpc:<hash>".
        # The path to matches is:
        #   ng_state["grpc:<hash>"]["response"]["payload"]["matches"]
        # We also do a recursive fallback search in case the structure changes.
        matches_found = []

        def _find_matches_recursive(obj, depth=0):
            """Recursively search for any list keyed 'matches' containing match objects."""
            if depth > 6 or not isinstance(obj, dict):
                return
            if "matches" in obj and isinstance(obj["matches"], list) and obj["matches"]:
                # Confirm at least the first item looks like a match (has isLive / odds)
                first = obj["matches"][0]
                if isinstance(first, dict) and ("isLive" in first or "markets" in first):
                    matches_found.extend(obj["matches"])
                    return   # don't recurse further into this branch
            for v in obj.values():
                if isinstance(v, dict):
                    _find_matches_recursive(v, depth + 1)

        for key, value in ng_state.items():
            if not isinstance(value, dict):
                continue
            # Primary path for gRPC keys: response → payload → matches
            if key.startswith("grpc:"):
                payload = (
                    value
                    .get("response", {})
                    .get("payload", {})
                )
                if "matches" in payload and isinstance(payload["matches"], list):
                    matches_found.extend(payload["matches"])
                    continue
            # Fallback: deep search for any "matches" array in this top-level value
            _find_matches_recursive(value)

        if not matches_found:
            print("[Betclic] No 'matches' array found in ng-state payload.")
            return quotes

        timestamp = datetime.now()

        for match in matches_found:
            try:
                is_live = match.get("isLive", False)

                # Apply live / pre-match filter
                if is_live and not CAPTURE_LIVE:
                    continue
                if not is_live and not CAPTURE_PREMATCH:
                    continue

                # ── Player names ──────────────────────────────────────────
                contestants = match.get("contestants", [])
                if len(contestants) >= 2:
                    player_1 = contestants[0].get("name", "Unknown")
                    player_2 = contestants[1].get("name", "Unknown")
                else:
                    # Fallback: split match name on " - "
                    name_parts = match.get("name", " - ").split(" - ", 1)
                    player_1 = name_parts[0].strip() if len(name_parts) > 0 else "Unknown"
                    player_2 = name_parts[1].strip() if len(name_parts) > 1 else "Unknown"

                match_id = f"{player_1}_vs_{player_2}"

                # ── Odds ──────────────────────────────────────────────────
                # Betclic uses a singular "market" object (not the "markets" list,
                # which may be empty for already-opened/live matches).
                # Fallback: try the first element of the "markets" list if needed.
                main_market = match.get("market") or (match.get("markets") or [None])[0]
                if main_market is None:
                    continue

                selections = main_market.get("mainSelections", [])

                if len(selections) < 2:
                    continue

                odds_1 = float(selections[0].get("odds", 0))
                odds_2 = float(selections[1].get("odds", 0))

                # Skip if odds are 0 or missing (market suspended)
                if odds_1 == 0 or odds_2 == 0:
                    continue

                quotes.append({
                    'timestamp': timestamp,
                    'bookmaker': 'Betclic',
                    'match_id': match_id,
                    'is_live': is_live,
                    'player_1': player_1,
                    'player_2': player_2,
                    'odds_1': odds_1,
                    'odds_2': odds_2,
                })

            except Exception as e:
                print(f"[Betclic] Error parsing match entry: {e}")
                continue

        print(f"[Betclic] Parsed {len(quotes)} match(es) successfully.")

    except requests.exceptions.RequestException as e:
        print(f"[Betclic] Network error: {e}")
    except Exception as e:
        print(f"[Betclic] Unexpected error: {e}")

    return quotes


# ──────────────────────────────────────────────────────────────────────────────
# Aggregator
# ──────────────────────────────────────────────────────────────────────────────

def fetch_all_quotes():
    """Fetch quotes from all configured bookmakers and return a DataFrame."""
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Scraping quotes...")

    betclic_data = get_betclic_quotes()
    all_quotes   = betclic_data          # extend this list to add more bookmakers

    if not all_quotes:
        return pd.DataFrame()

    return pd.DataFrame(all_quotes)


# ──────────────────────────────────────────────────────────────────────────────
# Persistence
# ──────────────────────────────────────────────────────────────────────────────

def save_quotes(df_new: pd.DataFrame):
    """
    Append new/changed quote rows to the Parquet file.
    Only rows whose odds have changed since the previous snapshot are saved.
    """
    if df_new.empty:
        print(" -> No quotes to save.")
        return

    if os.path.exists(PARQUET_FILE):
        existing_df = pd.read_parquet(PARQUET_FILE)

        # Latest snapshot per match_id
        latest = (
            existing_df
            .sort_values('timestamp')
            .groupby('match_id')
            .last()
            .reset_index()
        )

        rows_to_append = []
        for _, row in df_new.iterrows():
            prev = latest[latest['match_id'] == row['match_id']]
            if prev.empty:
                rows_to_append.append(row)                        # new match
            elif row['odds_1'] != prev.iloc[0]['odds_1'] or row['odds_2'] != prev.iloc[0]['odds_2']:
                rows_to_append.append(row)                        # odds changed

        if rows_to_append:
            append_df  = pd.DataFrame(rows_to_append)
            updated_df = pd.concat([existing_df, append_df], ignore_index=True)
            updated_df.to_parquet(PARQUET_FILE, engine='pyarrow')
            print(f" -> Appended {len(append_df)} new/changed row(s). Total: {len(updated_df)}.")
        else:
            print(" -> No odds changes detected, nothing written.")
    else:
        df_new.to_parquet(PARQUET_FILE, engine='pyarrow')
        print(f" -> Created '{PARQUET_FILE}' with {len(df_new)} row(s).")


# ──────────────────────────────────────────────────────────────────────────────
# Main loop
# ──────────────────────────────────────────────────────────────────────────────

def fetch_and_save(live_only: bool):
    """
    Fetch quotes and persist them.
    If live_only=True, only live rows are saved (fast path, runs every second).
    If live_only=False, both live and pre-match rows are saved (slow path).
    """
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Scraping quotes... "
          f"({'live only' if live_only else 'live + pre-match'})")

    betclic_data = get_betclic_quotes()
    if not betclic_data:
        print(" -> No quotes found or parsing failed.")
        return

    df_all = pd.DataFrame(betclic_data)

    if live_only:
        df = df_all[df_all['is_live'] == True].copy()
    else:
        df = df_all.copy()

    save_quotes(df)


def main():
    print("Starting live tennis quotes scraper...")
    print(f"Data will be stored in: {PARQUET_FILE}")
    print(f"Live refresh    : every {LIVE_INTERVAL_SECONDS}s")
    print(f"Pre-match refresh: every {PREMATCH_INTERVAL_SECONDS}s")
    print("Press Ctrl+C to stop.\n")

    ticks_since_prematch = PREMATCH_INTERVAL_SECONDS  # run pre-match on first tick too

    while True:
        try:
            run_prematch = (ticks_since_prematch >= PREMATCH_INTERVAL_SECONDS)
            fetch_and_save(live_only=not run_prematch)
            if run_prematch:
                ticks_since_prematch = 0
            else:
                ticks_since_prematch += LIVE_INTERVAL_SECONDS
        except KeyboardInterrupt:
            print("\nStopped by user.")
            break
        except Exception as e:
            print(f"Critical error in main loop: {e}")

        time.sleep(LIVE_INTERVAL_SECONDS)


if __name__ == "__main__":
    main()
