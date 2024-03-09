import asyncio
import pickle
from tenacity import retry, wait_exponential
import polars
from aiohttp import ClientSession
from limiter import Limiter
from tqdm.asyncio import tqdm_asyncio

# Extract from full_text renewals
CONTENT = lambda full_text: (
    f"The following is extract from copyright renewals. Please extract the fields in the "
    f"required format. If a field is ambiguous return `None`. The return should be a json. "
    f"Do not return anything extraneous\n\nauthor_names: list: [str(Last Name, First Name, "
    f"Middle)] or <Organization name>\ntitle: str\nregistration_numbers: list: starting "
    f"with A-C. AI is interim and A is final\nregistration_dates: list: "
    f"<YYYY-MM-DD>\nrenewal_numbers: list: start with R\nclaimants: list\n\n{full_text}"
)

endpoint = "https://api.together.xyz/v1/chat/completions"
data = polars.read_parquet(
    "/Users/baber/PycharmProjects/cce-python/extract_from_agi_uuid.parquet"
).to_dicts()  # your data here
data = data
start, end = 6000, 0
data = data[start:]
# Initialize your limiter here
limit_downloads = Limiter(rate=50, capacity=50, consume=1)


@limit_downloads
@retry(wait=wait_exponential(multiplier=1, min=4, max=10))
async def post_request(
    session, json_data: dict, headers: dict, res: str
) -> tuple[dict, str]:
    try:
        async with session.post(endpoint, json=json_data, headers=headers) as response:
            return await response.json(), res
    except Exception as e:
        print(f"An error occurred while making a request: {e}")
        return {}, res


async def main():
    tasks = []  # Here you store the tasks
    async with ClientSession() as session:
        for x in data:
            uuid = x["uuid"]
            full_text = x.get("full_text")
            if full_text:
                json_data = {
                    "model": "NousResearch/Nous-Hermes-2-Mistral-7B-DPO",
                    "max_tokens": 256,
                    "temperature": 0.7,
                    "top_p": 0.7,
                    "top_k": 50,
                    "repetition_penalty": 1,
                    "stop": ["<|im_end|>"],
                    "messages": [
                        {
                            "content": "You are a helpful and meticulous librarian with experience of reading and recommending books",
                            "role": "system",
                        },
                        {"content": CONTENT(full_text), "role": "user"},
                    ],
                    "repetitive_penalty": 1,
                }
                headers = {
                    "Authorization": "Bearer b94524890c4ca82c840b2d55b0ce6d47c2d211816cd4be509eb14b8abbff1396",
                }
                task = asyncio.ensure_future(
                    post_request(session, json_data, headers, uuid)
                )  # Make a task for post request
                tasks.append(task)  # Add the task to tasks list
    responses = await tqdm_asyncio.gather(*tasks)
    return responses


# Run the main coroutine
if __name__ == "__main__":
    results = asyncio.run(main())
    with open("results.pkl", "wb") as f:
        pickle.dump(results, f)
