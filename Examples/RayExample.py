import os
import time

import numpy as np
import pandas as pd
import ray
from joblib import Parallel, delayed
from ray.util.joblib import register_ray  # noqa: E402


# lets define a toy problem:
def costly_simulation(list_param):
    time.sleep(np.random.random())
    return sum(list_param)

input_params = pd.DataFrame(np.random.random(size=(500, 4)),
                            columns=['param_a', 'param_b', 'param_c', 'param_d'])

# now we use the ray cluster to run the problem:
ray.init(address=os.getenv("RAY_ADDRESS"), _redis_password=os.getenv("REDIS_PWD"))
register_ray()
print("Starting ray loop:")
with parallel_backend("ray"):
    parallel_results = Parallel(verbose=7, n_jobs=-1)(
        delayed(costly_simulation)(input_param[1].values) for input_param in tqdm(input_params))
    
with open("ray_exampel_output", "w") as f:
    for result in parallel_results:
        f.write(result)
        f.write("\n")