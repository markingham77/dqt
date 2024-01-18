import pandas as pd
import numpy as np
from itertools import combinations

def dqt_combinations(iterable):
    result = []
    for k in range(1, len(iterable) + 1):
        result.append(list(combinations(iterable, k)))
    return result