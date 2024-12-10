import pickle
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from kaggle.api.kaggle_api_extended import KaggleApi
from kaggle.rest import ApiException
import tqdm
import os

# authentification
api = KaggleApi()
api.authenticate()

API_LIMIT = 200 # max requests per minute
MAX_PAGES = 100  # max pages
MAX_WORKERS = 10
OUTPUT_DIR = "../data"
metadata = []    # metadata list
errors = []      # error list

def fetch_page(page):
    try:
        results = api.dataset_list(
            sort_by="hottest",  
            file_type="csv",     
            license_name="all", 
            page=page,           
        )
        time.sleep(60 * MAX_WORKERS / API_LIMIT)
        return results
    except ApiException as e:
        return {"error": e, "page": page}

with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
    futures = {executor.submit(fetch_page, page): page for page in range(1, MAX_PAGES + 1)}
    for future in tqdm.tqdm(as_completed(futures), total=len(futures)):
        result = future.result()
        if isinstance(result, list):
            metadata.extend(result)  # save success
        else:
            errors.append(result)    # save errors

if not os.path.exists(OUTPUT_DIR):
    os.makedirs(OUTPUT_DIR)

# save results
with open(OUTPUT_DIR + "/kaggle_metadata.pkl", "wb") as file:
    pickle.dump(metadata, file)

with open(OUTPUT_DIR + "/kaggle_errors.pkl", "wb") as file:
    pickle.dump(errors, file)

print(f"{len(metadata)} metadata collected")
print(f"{len(errors)} errors occurred")
