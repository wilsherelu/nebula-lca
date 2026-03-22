import numpy as np


def compute_lcia(a_matrix: dict, b_matrix: dict, c_matrix: dict) -> np.ndarray:
    n = a_matrix["shape"][0]
    m = b_matrix["shape"][0]
    k = c_matrix["shape"][0]

    a = np.zeros((n, n), dtype=float)
    for entry in a_matrix["data"]:
        a[entry["row_index"], entry["col_index"]] = entry["value"]

    b = np.zeros((m, n), dtype=float)
    for entry in b_matrix["data"]:
        b[entry["row_index"], entry["col_index"]] = entry["value"]

    c = np.zeros((k, m), dtype=float)
    for entry in c_matrix["data"]:
        c[entry["row_index"], entry["col_index"]] = entry["value"]

    a_inv = np.linalg.inv(a)
    return c @ (b @ a_inv)
