import asyncio
import datetime
import json
from asyncio import Semaphore
from pathlib import Path

import aiohttp
from tqdm.asyncio import tqdm

MAX_CONCURRENT = 4
COUNT = 10000


class PaginatedRequest:
    def __init__(
            self,
            session: aiohttp.client.ClientSession,
            start_date: str,
            end_date: str,
            params: dict = None
    ):
        self.retries = 0
        self.session = session
        self.max_retries = 3
        self.start_date = start_date
        self.end_date = end_date
        self.base_url = "https://archive.org/services/search/v1/scrape"
        if params:
            self.params = params
        else:
            self.params = dict(
                q=f"date:[{self.start_date} TO {self.end_date}] and mediatype:texts",
                count=str(COUNT),
                fields="identifier,date,year,creator,language,title,licenseurl,call_number,createddate,imagecount,stars,avg_rating,creatorSorter,titleSorter,publicdate",
                sorts="publicdate desc",
            )
        self.cursor = None

    async def fetch_pages(self):
        while True:
            if self.cursor:
                self.params["cursor"] = self.cursor

            async with self.session.get(
                    self.base_url, params=self.params
            ) as response:
                if response.status == 200:
                    data = await response.json()
                    items = data.get("items", [])

                    if items or self.retries >= self.max_retries:
                        # Reset retries counter on successful fetch or after hitting max retries
                        self.retries = 0

                        # Yield items if available or after max retries to prevent infinite loop
                        yield items
                        self.cursor = data.get("cursor")
                        if not self.cursor:
                            break
                    else:
                        self.retries += 1
                        print(f"Retry {self.retries}/{self.max_retries} for empty items list.")
                        await asyncio.sleep(1)

                else:
                    print(
                        f"Error fetching page: HTTP Status {response.status} - {response.reason}"
                    )
                    break


def split_dates(split: int = MAX_CONCURRENT) -> list[tuple[str, str]]:
    cutoff_year = datetime.datetime.now(datetime.UTC).year - 95 - 10
    start_year = cutoff_year
    end_year = 1973  # Fixed end year

    # Calculate the total period and divide into 10 intervals
    total_years = end_year - start_year
    interval_length = total_years // split

    # Generate non-overlapping date ranges
    return_dates = []
    for i in range(split):
        interval_start = start_year + i * interval_length
        # Ensure the last interval goes up to the end_year
        if i == split - 1:
            interval_end = end_year
        else:
            interval_end = (
                    start_year + (i + 1) * interval_length - 1
            )  # Subtract 1 to avoid overlap
        return_dates.append((f"{interval_start}-01-01", f"{interval_end}-12-31"))

    return return_dates


async def process_pages(semaphore, paginated_request: PaginatedRequest, pbar: tqdm) -> list:
    async with semaphore:
        items = []  # List to accumulate JSON items
        async for page in paginated_request.fetch_pages():
            for item in page:
                if item is not None:
                    json_item = json.dumps(item) + "\n"
                    items.append(json_item)
                    pbar.update(1)
        return items


async def main():
    date_ranges = split_dates()
    semaphore = Semaphore(MAX_CONCURRENT)
    res = []
    async with aiohttp.ClientSession() as session:
        with tqdm(total=2146141, desc="Fetching pages") as pbar:
            tasks = [
                asyncio.create_task(
                    process_pages(semaphore,
                                  PaginatedRequest(session=session, start_date=start, end_date=end), pbar
                                  )
                )
                for start, end in date_ranges
            ]
            results = await asyncio.gather(*tasks)
            for items in results:
                res.extend(items)
    return res


if __name__ == "__main__":
    all_items = asyncio.run(main())
    output = Path("output/ia-0-texts.ndjson")
    output.parent.mkdir(parents=True, exist_ok=True)
    with open(output, "w") as file:
        file.writelines(all_items)
