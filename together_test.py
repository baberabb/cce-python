import ast
import re
import asyncio
import pickle
from tenacity import retry, wait_exponential
import polars
from aiohttp import ClientSession
from limiter import Limiter
from tqdm.asyncio import tqdm_asyncio

# Extract from full_text renewals
# CONTENT = lambda full_text: (f"The following is extract from copyright renewals. Please extract the fields in the "
#                              f"required format. If a field is ambiguous return `None`. The return should be a json. "
#                              f"Do not return anything extraneous\n\nauthor_names: list: [str(Last Name, First Name, "
#                              f"Middle)] or <Organization name>\ntitle: str\nregistration_numbers: list: starting "
#                              f"with A-C. AI is interim and A is final\nregistration_dates: list: "
#                              f"<YYYY-MM-DD>\nrenewal_numbers: list: start with R\nclaimants: list\n\n{full_text}")
PROMPT = """The following is a book entry along with probable matching entries in a library system. One per line. Your job is to classify if there is a match of the entry with the choices. Consider both the title and and the author when matching. If none given then only match if there is a high possibility. Err on the wrong matching side. Explain why it is a match and MUST respond in the following format:\n\n{"match": [$Character(s) of ALL matching references or None if no match]}\n\nEntry:\nWords and phrases, 1658 to date. by West publishing co.\n\nChoices:\nA. [WORDS AND PHRASES, PERMANENT EDITION by West Pub. Co. (PWH)]\nB. [WORDS AND PHRASES, PERMANENT EDITION. Vol. 11. by]<|im_end|>\n<|im_start|>assistant\nAt least one of the choices match so answer cannot be None. The titles are almost exactly similar and although there is no author in B, this clearly is the same book\n{\'match\': [\'A\', \'B\']}<|im_end|>\n<|im_start|>user\nEntry:\n"""


endpoint = "https://api.together.xyz/v1/chat/completions"
# Initialize your limiter here
limit_downloads = Limiter(rate=50, capacity=50, consume=1)


@limit_downloads
@retry(wait=wait_exponential(multiplier=1, min=4, max=10))
async def post_request(json_data: dict, headers: dict, res: dict) -> tuple[dict, dict]:
    async with ClientSession() as session:
        try:
            async with session.post(
                endpoint, json=json_data, headers=headers
            ) as response:
                return await response.json(), res
        except Exception as e:
            print(f"An error occurred while making a request: {e}")
            return {}, res


async def main(data: dict) -> tuple[dict, dict]:
    tasks = []  # Here you store the tasks
    for array_ in data:
        prompt = PROMPT
        prompt += array_["prompt_reg"]
        prompt += "\n\n"
        prompt += "Choices:"
        options = ["A", "B", "C", "D"]
        prompt += "\n" + "A" + ". [" + re.sub(r"\s+", " ", array_["prompt_ren"]) + "]"
        prompt += "\n"
        if prompt:
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
                    {"content": prompt, "role": "user"},
                ],
                "repetitive_penalty": 1,
            }
            headers = {
                "Authorization": "Bearer b94524890c4ca82c840b2d55b0ce6d47c2d211816cd4be509eb14b8abbff1396",
            }
            task = asyncio.ensure_future(
                post_request(json_data, headers, array_)
            )  # Make a task for post request
            tasks.append(task)  # Add the task to tasks list
    responses = await tqdm_asyncio.gather(
        *tasks
    )  # This line will start all tasks concurrently
    return responses


# Run the main coroutine
if __name__ == "__main__":
    data = polars.read_parquet(
        "/Users/baber/PycharmProjects/cce-python/llm/test_matching/renewals_unmatched_for_llm.parquet"
    ).to_dicts()  # your data here
    data = data
    start, end = 1, 5
    data = data
    results = asyncio.run(main(data))
    with open("llm/reg_unmatched_from_llm.pkl", "wb") as f:
        pickle.dump(results, f)
