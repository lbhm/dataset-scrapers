import pickle
import time

import kaggle
import pandas as pd
import tqdm
from kaggle.rest import ApiException

kaggle.api.authenticate()

username_slug = pd.read_csv("../data/username_slug.csv")

metadata = []
errors = []
for user in tqdm.tqdm(username_slug["UserName"].unique()):
    try:
        page = 1
        while True:
            results = kaggle.api.dataset_list(
                sort_by="hottest",
                file_type="csv",
                license_name="all",
                user=user,
                page=page,
            )
            if len(results) >= 1:
                metadata.extend(results)
            if len(results) < 10:
                break

            page += 1
            if page > 500:
                print(
                    f"User {user} has more than 10000 datasets. The API won't show all of them."
                )

            time.sleep(0.1)
    except ApiException as e:
        if e.reason == "Unauthorized":
            raise e
        else:
            errors.append((user, e))

with open("../data/kaggle_metadata.pkl", "wb") as file:
    pickle.dump(metadata, file)

with open("../data/kaggle_errors.pkl", "wb") as file:
    pickle.dump(errors, file)
