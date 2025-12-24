import csv
import threading
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import List, Iterable, Tuple

import concurrent.futures
import lzma
import numpy as np
import pandas as pd
import requests
import typer
from rich.console import Console
from rich.progress import track
from rich.table import Table
from typing_extensions import Annotated

app = typer.Typer()
DOWNLOAD_PATH = "./download/"
EXPORT_PATH = "./export/"
ERROR_LOG_LOCK = threading.Lock()

@app.command()
def download(assets:Annotated[List[str], typer.Argument(help="Give a list of assets to download. Eg. EURUSD AUDUSD")],
             start:Annotated[str, typer.Argument(help="Start date to download in YYYY-MM-DD format. Eg. 2024-01-08")],
             end:Annotated[str, typer.Option(help="End date to download in YYYY-MM-DD format. If not provided, will download until current date Eg. 2024-01-08")]="",
             concurrent:Annotated[int, typer.Option(help="Max number of concurrent downloads (defaults to max number of threads + 4 or 32 (which ever is less)) (Sometimes using too high of a number results in missing files)")]=0,
             force:Annotated[bool, typer.Option(help="Redownload files. By default, without this flag, files that already exist will be skipped")]=False,
             error_log:Annotated[Path, typer.Option(help="File to append download errors for later re-downloads")]=Path("download_errors.log")):
    """
    Download assets
    """
    start_date_str = start.split("-")
    end_date = datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(hours=1)
    base_url = "https://datafeed.dukascopy.com/datafeed/"

    if end != "":
        end_date = end.split("-")
        end_date = datetime(int(end_date[0]), int(end_date[1]), int(end_date[2]))

    processes = None
    if concurrent > 0:
        processes = concurrent

    delta = timedelta(hours=1)
    for asset in assets:
        filenames = []
        urls = []
        forces = []

        start_date = datetime(int(start_date_str[0]), int(start_date_str[1]), int(start_date_str[2]))
        while start_date <= end_date:
            year = start_date.year
            month = start_date.month-1
            day = start_date.day
            hour = start_date.hour

            filenames.append(Path(f"{DOWNLOAD_PATH}{asset}/{year}/{month:0>2}/{day:0>2}/{hour:0>2}h_ticks.bi5"))
            urls.append(f"{base_url}{asset}/{year}/{month:0>2}/{day:0>2}/{hour:0>2}h_ticks.bi5")
            forces.append(force)

            start_date += delta
        inputs = tuple(zip(filenames, urls, forces))
        download_file_parallel(inputs, asset, len(filenames), processes, error_log)
    print("Download completed")

def _session_with_retries() -> requests.Session:
    session = requests.Session()
    adapter = requests.adapters.HTTPAdapter(pool_connections=8, pool_maxsize=16, max_retries=2)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    session.headers.update({"User-Agent": "duka-downloader/1.0"})
    return session

def download_file_parallel(file_url_zip: Iterable[Tuple[Path, str, bool]], asset:str, length:int, processes_num=None, error_log:Path=Path("download_errors.log")):
    with _session_with_retries() as session:
        with concurrent.futures.ThreadPoolExecutor(max_workers=processes_num) as executor:
            results = [executor.submit(download_file, args, asset, error_log, session) for args in file_url_zip]
            for _ in track(concurrent.futures.as_completed(results), total=length, description=f"Downloading {asset}..."):
                pass

def log_error(error_log: Path, asset: str, url: str, target: Path, reason: str) -> None:
    error_log.parent.mkdir(exist_ok=True, parents=True)
    row = [
        datetime.utcnow().isoformat(),
        asset,
        str(target),
        url,
        reason,
    ]
    with ERROR_LOG_LOCK:
        exists = error_log.exists()
        with open(error_log, "a", newline="") as f:
            writer = csv.writer(f)
            if not exists:
                writer.writerow(["timestamp_utc", "asset", "file", "url", "reason"])
            writer.writerow(row)

def download_file(args, asset: str, error_log: Path, session: requests.Session):
    filename, url, is_force = args[0], args[1], args[2]

    if filename.exists() and not is_force:
        return

    try:
        with session.get(url, timeout=20, stream=True) as r:
            status = r.status_code
            if status != 200:
                log_error(error_log, asset, url, filename, f"bad_status:{status}")
                return

            # Avoid writing empty or suspiciously small responses.
            content_length = r.headers.get("Content-Length")
            if content_length is not None and int(content_length) == 0:
                log_error(error_log, asset, url, filename, "empty_content_length")
                return

            filename.parent.mkdir(exist_ok=True, parents=True)
            with open(filename, "wb") as f:
                for chunk in r.iter_content(chunk_size=1024 * 256):
                    if chunk:
                        f.write(chunk)
                f.flush()
    except Exception as exc:
        log_error(error_log, asset, url, filename, f"request_failed:{exc}")
        return


@app.command()
def export(assets:Annotated[List[str], typer.Argument(help="Give a list of assets to export. Use 'all' for all downloaded assets. Eg. EURUSD AUDUSD. Check export --help for more info")],
           timeframe:Annotated[str, typer.Argument(help="Timeframe to export. Format should be [Number][Unit] eg. 1h or 1t. Check export --help for more info about units.")],
           start:Annotated[str, typer.Argument(help="Start date to export in YYYY-MM-DD format. Eg. 2024-01-08")],
           end:Annotated[str, typer.Option(help="End date to export in YYYY-MM-DD format. If not provided, will export until current date Eg. 2024-01-08")]=""):
    """
    Export downloaded data into different timeframes/units.\n
    assets can be selected by listing multiple with a space dividing them or a single asset.\n
    Eg. export AUDUSD EURUSD\n
    Can also use all to select all downloaded assets.\n
    Eg. export all\n
    Available units:\n
        t: ticks (eg. 1t)\n
        s: seconds (eg. 10s)\n
        m: minutes (eg. 15m)\n
        h: hours (eg. 4h)\n
        D: days (eg. 2D)\n
        W: weeks (eg. 2W)\n
    """
    asset_list = []
    if assets[0] == "all":
        dirs = Path(DOWNLOAD_PATH).glob("*")
        for dir in dirs:
            parts = dir.parts
            asset_list.append(parts[1])
    else:
        asset_list = assets

    start_date_str = start.split("-")
    end_date = datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(hours=1)

    if end != "":
        end_date = end.split("-")
        end_date = datetime(int(end_date[0]), int(end_date[1]), int(end_date[2]))

    delta = timedelta(hours=1)
    for asset in asset_list:
        filenames = []
        file_times = []

        start_date = datetime(int(start_date_str[0]), int(start_date_str[1]), int(start_date_str[2]))
        while start_date <= end_date:
            year = start_date.year
            month = start_date.month-1
            day = start_date.day
            hour = start_date.hour

            filenames.append(Path(f"{DOWNLOAD_PATH}{asset}/{year}/{month:0>2}/{day:0>2}/{hour:0>2}h_ticks.bi5"))
            file_times.append(datetime(year, month+1, day, hour))

            start_date += delta

        df_list = []
        
        for i in track(range(len(filenames)), description=f"Reading {asset} tick files..."):
            file = filenames[i]
            if not file.is_file():
                print(f"{file} is missing, skipping this file.")
                continue
            if file.stat().st_size == 0:
                continue
            dt = np.dtype([('TIME', '>i4'), ('ASKP', '>i4'), ('BIDP', '>i4'), ('ASKV', '>f4'), ('BIDV', '>f4')])
            data = np.frombuffer(lzma.open(file, mode="rb").read(),dt)
            df = pd.DataFrame(data)
            df["TIME"] = pd.to_datetime(df["TIME"], unit="ms", origin=file_times[i])
            df_list.append(df)

        df = pd.concat(df_list, ignore_index=True)
        df["ASKP"] = df["ASKP"].astype(np.int32)
        df["BIDP"] = df["BIDP"].astype(np.int32)
        df["ASKV"] = df["ASKV"].astype(np.float32)
        df["BIDV"] = df["BIDV"].astype(np.float32)

        df["ASKP"] = df["ASKP"] / 100_000
        df["BIDP"] = df["BIDP"] / 100_000

        console = Console()
        with console.status(f"Aggregating {asset} data...") as status:
            agg_df = aggregate_data(df, timeframe)
            console.print(f"{asset} data aggregated")

            status.update(f"Exporting {asset} to file...")

            export_file = Path(f"{EXPORT_PATH}{asset}.csv")
            export_file.parent.mkdir(exist_ok=True, parents=True)
            agg_df.to_csv(export_file, index=False)
            console.print(f"{asset} exported to {export_file}")

    print(f"Export completed. Data located at {Path(EXPORT_PATH).resolve()}")

def aggregate_data(df:pd.DataFrame, tf:str):
    if "t" in tf:
        tick_num = int(tf.split("t")[0])
        if tick_num == 1:
            return df
        df_group = df.groupby(df.index // tick_num)
        agg_df = pd.DataFrame()
        agg_df["date"] = df_group["TIME"].first()
        agg_df["open"] = df_group["BIDP"].first()
        agg_df["high"] = df_group["BIDP"].max()
        agg_df["low"] = df_group["BIDP"].min()
        agg_df["close"] = df_group["BIDP"].last()
        agg_df["vol"] = df_group["BIDV"].sum()
        return agg_df

    agg_time = pd.Timedelta(tf)

    df = df.set_index("TIME")
    df_group = df.resample(agg_time)

    agg_df = pd.DataFrame()
    agg_df["open"] = df_group["BIDP"].first()
    agg_df["high"] = df_group["BIDP"].max()
    agg_df["low"] = df_group["BIDP"].min()
    agg_df["close"] = df_group["BIDP"].last()
    agg_df["vol"] = df_group["BIDV"].sum()
    agg_df = agg_df.reset_index(names="date")

    agg_df = agg_df.dropna()
    return agg_df

@app.command("list")
def list_command():
    """
    List all downloaded assets
    """
    assets = grab_asset_dirs()

    table = Table(title="Downloaded Data")

    table.add_column("Asset")
    table.add_column("Start Date (YYYY-MM-DD)")
    table.add_column("End Date (YYYY-MM-DD)")

    for asset in assets:
        table.add_row(asset, min(assets[asset]).strftime("%Y-%m-%d"), max(assets[asset]).strftime("%Y-%m-%d"))

    console = Console()
    console.print(table)
    console.print(f"Total Number of Assets: {len(assets)}")

@app.command()
def update(assets:Annotated[List[str], typer.Argument(help="Give a list of assets to update. Use 'all' for all downloaded assets. Eg. EURUSD AUDUSD. Check update --help for more info")],
           start:Annotated[str, typer.Option(help="Start date to update from in YYYY-MM-DD format. This overrides the default which uses the latest downloaded file as the start date. Eg. 2024-01-08")]="",
           concurrent:Annotated[int, typer.Option(help="Max number of concurrent downloads (defaults to max number of threads + 4 or 32 (which ever is less)) (Sometimes using too high of a number results in missing files)")]=0,
           force:Annotated[bool, typer.Option(help="Redownload files. By default, without this flag, files that already exist will be skipped. This can be used with --start to force redownload.")]=False):
    """
    Update downloaded assets to latest date.\n
    assets can be selected by listing multiple with a space dividing them or a single asset.\n
    Eg. export AUDUSD EURUSD\n
    Can also use all to select all downloaded assets.\n
    Eg. export all\n
    """
    assets_dict = grab_asset_dirs()
    if assets[0] != "all":
        for asset in assets:
            start_date = max(assets_dict[asset])
            if start != "":
                start_split = start.split("-")
                start_date = datetime(int(start_split[0]), int(start_split[1]), int(start_split[2]))
            download([asset], start_date.strftime("%Y-%m-%d"), concurrent=concurrent, force=force)
        return

    for asset in assets_dict:
        start_date = max(assets_dict[asset])
        if start != "":
            start_split = start.split("-")
            start_date = datetime(int(start_split[0]), int(start_split[1]), int(start_split[2]))
        download([asset], start_date.strftime("%Y-%m-%d"), concurrent=concurrent, force=force)


def grab_asset_dirs():
    dirs = Path(DOWNLOAD_PATH).glob("*/*/*/*")
    assets = {}
    for dir in dirs:
        parts = dir.parts
        asset = parts[1]
        if asset not in assets:
            assets[asset] = []
        assets[asset].append(datetime(int(parts[2]), int(parts[3])+1, int(parts[4])))
    return assets

if __name__ == "__main__":
    app()