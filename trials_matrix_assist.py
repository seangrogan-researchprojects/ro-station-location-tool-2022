import collections
import os

import numpy as np
import pandas as pd


def _do_stuff(folder, base_path):
    row = folder.split("_")
    deptFail = int(row[3][:3])
    tgtFeas = int(row[5][:3])
    best_soln_path = f"{base_path}{folder}/solutions"
    solution_file = sorted(os.listdir(best_soln_path))[0]
    nStations = int(solution_file.split("_")[1])
    return deptFail, tgtFeas, nStations


def get_completed_data(base_path):
    if isinstance(base_path, list):
        completed_data = []
        for p in base_path:
            completed_data += get_completed_data(p)
    else:
        folders = os.listdir(base_path)
        Trial = collections.namedtuple("Trials", ["deptFail", "tgtFeas", "nStations"])
        completed_data = [Trial(*_do_stuff(folder, base_path)) for folder in folders]
    completed_data = sorted(sorted(completed_data, key=lambda x: x[1]))
    return completed_data
    
def trial_matrix_assist(base_path='./outputs/'):
    completed_data = get_completed_data(base_path)
    for element in completed_data:
        print(element)
    data_for_pd = {i: {j: np.nan for j in range(100, 79, -1)} for i in range(0, 21)}
    for deptFail, tgtFeas, nStations in completed_data:
        data_for_pd[deptFail][tgtFeas] = nStations

    df = pd.DataFrame(data_for_pd).T
    df.to_csv("temp.csv")


if __name__ == '__main__':
    trial_matrix_assist(["./outputs/", "G:/DisasterLocationTool/outputs/"])
