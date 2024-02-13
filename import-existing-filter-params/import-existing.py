import datetime
from pathlib import Path
import sys
import uuid

import click
import pandas as pd

pd.set_option("mode.copy_on_write", True)


@click.command()
@click.option("--seaflow-sfl", "-s",
    type=click.Path(file_okay=False, dir_okay=True, readable=True, path_type=Path),
    required=True,
    help="seaflow-sfl git repo clone dir"
)
@click.option(
    "--seaflow-filter", "-f",
    type=click.Path(file_okay=False, dir_okay=True, readable=True, path_type=Path),
    required=True,
    help="seaflow-filter git repo clone dir"
)
@click.option(
    "--out-dir", "-o",
    type=click.Path(writable=True, readable=True, path_type=Path),
    required=True,
    help="Output dir"
)
def run(seaflow_sfl: Path, seaflow_filter: Path, out_dir: Path):
    try:
        starts = get_starts(seaflow_sfl)
    except Exception as e:
        raise click.ClickException(f"Reading cruise start dates failed with: {e}")
    try:
        params = get_params(seaflow_filter)
    except Exception as e:
        raise click.ClickException(f"Reading filter parameters failed with: {e}")
    try:
        params_n = write_params(starts, params, out_dir)
    except Exception as e:
        raise click.ClickException(f"Writing filter parameters failed with: {e}")

    print(f"Wrote filter parameters for {params_n} cruises", file=sys.stderr)

def get_starts(seaflow_sfl: Path) -> dict[str, str]:
    starts: dict[str, str] = dict()
    for sfl in seaflow_sfl.glob("curated/*.sfl"):
        cruise = sfl.name[:sfl.name.rindex("_")]
        df = pd.read_csv(sfl, sep="\t", dtype=object)
        starts[cruise] = df["DATE"].sort_values().iat[0]
    return starts


def get_params(seaflow_filter: Path) -> dict[str, pd.DataFrame]:
    params: dict[str, pd.DataFrame] = dict()
    for path in seaflow_filter.glob("*"):
        if path.is_dir():
            param_file = path / "filterparams.csv"
            if param_file.exists():
                df = pd.read_csv(param_file)
                # Normalize to '_' for word separator in column names
                df.columns = [c.replace(".", "_") for c in df.columns]
                cruise = path.name
                params[cruise] = df
    return params


def write_params(starts: dict[str, str], params: dict[str, pd.DataFrame], out_dir: Path) -> int:
    cruises = out_dir / "cruises"
    cruises.mkdir(parents=True, exist_ok=False)
    now = datetime.datetime.now(tz=datetime.timezone.utc).isoformat(timespec="seconds")
    count = 0
    for cruise, param_df in params.items():
        cruise_dir = cruises / cruise
        cruise_dir.mkdir(exist_ok=False)

        # Write filter params TSV
        filter_id = str(uuid.uuid4())
        new_param_df = param_df.copy()
        new_param_df.insert(2, "date", now)
        new_param_df.insert(2, "id", filter_id)
        new_param_df.to_csv(
            f"{cruise_dir}/{cruise}.filter_params.filter.tsv",
            sep="\t",
            index=False
        )

        # Write filter plan TSV
        plan_df = pd.DataFrame(
            {"start_date": [starts[cruise]], "filter_id": [filter_id]},
            dtype=object
        )
        plan_df.to_csv(
            f"{cruise_dir}/{cruise}.filter_params.filter_plan.tsv",
            sep="\t",
            index=False
        )
        count += 1
    return count


if __name__ == "__main__":
    run()
