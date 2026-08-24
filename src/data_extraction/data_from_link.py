import argparse
from datetime import date
from typing import Iterable
import requests
from dateutil.relativedelta import relativedelta
from pathlib import Path


# urls for data download
BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/"
taxi_type_url_dict = {
    "yellow": "yellow_tripdata",
    "green": "green_tripdata",
    "for hire": "fhv_tripdata",
    "for hire high volume": "fhvhv_tripdata",
}
PROJECT_PATH = Path(__file__).resolve().parent.parent.parent


def download_data(year:int, month:int, taxi_type:str) -> None:
    download_url = f"{BASE_URL}{taxi_type}_{year}-{month:02d}.parquet"
    response = requests.get(download_url)

    file_path = PROJECT_PATH / "taxi_data" / f"{taxi_type}" / f"{taxi_type}_taxi_data_{year}_{month}.parquet"
    file_path.parent.mkdir(parents=True, exist_ok=True)

    print(response.status_code, response)
    if response.status_code == 200:
        with open(file_path, "wb") as f:
            f.write(response.content)
    else:
        print(f"error downloading: {response.status_code} {response.content}")


def set_time_window(months: int) -> Iterable[date]:
    """
    return 3 months prior and onwards data
    """
    current_date = date.today()
    last_date = current_date - relativedelta(months=3)
    first_date = last_date - relativedelta(months=months)
    print(f"{first_date} --> {last_date}")

    diff = relativedelta(last_date, first_date)
    diff_months = diff.months + diff.years*12

    dates = []

    for month in range(1, diff_months+1):
        date_to_add = first_date + relativedelta(months=month)
        dates.append(date_to_add)
    return dates



def main():
    """
    run the downloader
    """
    parser = argparse.ArgumentParser(
        description="Download taxi trip data",
    )
    parser.add_argument("--tp-months", type=int, default=24)
    args = parser.parse_args()

    tp_months = args.tp_months

    if tp_months < 1 or tp_months > 180:
        raise ValueError("TP_MONTHS must be greater than 0 and less than or equal to 180")

    dates = set_time_window(months=tp_months)
    for date in dates:
        download_data(date.year, date.month, taxi_type_url_dict["yellow"])


if __name__ == "__main__":
    main()