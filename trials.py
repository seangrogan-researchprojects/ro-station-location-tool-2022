import datetime
import os
import random

from main import main
from trials_matrix_assist import get_completed_data, trial_matrix_assist
from utilities import parfile_reader, parfile_writer


def trials(base_parfile, end="mon", base_paths=None):
    if base_paths is None:
        base_paths = ["./outputs/", "G:/DisasterLocationTool/outputs/"]
    depot_failure_rates = list(range(0, 21, 1))
    target_feasibility_pcts = [100]
    cases = []
    for depot_failure_rate in depot_failure_rates:
        for target_feasibility_pct in target_feasibility_pcts:
            deptFail, tgtFeas, nStations = zip(*get_completed_data(base_paths))
            if (depot_failure_rate, target_feasibility_pct) in set(zip(deptFail, tgtFeas)):
                continue
            cases.append((
                dict(
                    name=f"Oklahoma_deptFail_{depot_failure_rate:0>3}pct_"
                         f"tgtFeas_{target_feasibility_pct:0>3}pct",
                    depot_failure_rate=round(depot_failure_rate / 100, 2),
                    target_feasibility_pct=round(target_feasibility_pct / 100, 2)
                ), (depot_failure_rate, target_feasibility_pct))
            )
            if depot_failure_rate == 0:
                cases[-1][0]["num_tests_each_date"] = 1
    random.shuffle(cases)

    start_date_time = datetime.datetime.now()
    day_inc = 1
    if end.lower() in {'tomorrow', 'tom'}:
        end_date_time = start_date_time.replace(hour=5, minute=30, second=0, microsecond=0)
        day_inc = 1
        end_date_time = end_date_time.replace(day=end_date_time.day + day_inc)
    elif end.lower() in {'monday', 'mon'}:
        end_date_time = start_date_time.replace(hour=4, minute=30, second=0, microsecond=0)
        day_inc = datetime.timedelta(days=-start_date_time.weekday(), weeks=1)
        end_date_time += day_inc

    for case, (depot_failure_rate, target_feasibility_pct) in cases:
        deptFail, tgtFeas, nStations = zip(*get_completed_data(base_paths))
        if (depot_failure_rate, target_feasibility_pct) in set(zip(deptFail, tgtFeas)):
            continue
        if end_date_time <= datetime.datetime.now():
            print(f"ending via Time Break {datetime.datetime.now().isoformat()}")
            break
        parfile = parfile_reader(base_parfile)
        parfile.update(case)
        path, filename = os.path.split(base_parfile)
        new_file_name = f"{path}/{case['name']}/par_{case['name']}.json"
        print(f"writing new parfile {new_file_name}")
        parfile_writer(new_file_name, parfile)
        main(parfile=new_file_name)
        try:
            trial_matrix_assist(base_path='./outputs/')
        except:
            pass
    return 0


if __name__ == '__main__':
    trials(base_parfile='./par_files/par2_for_tests.json')
    # main(parfile='./par_files/par2_for_tests.json',
    #      pickles='./pickles/pickles.json',
    #      pickle_case='300_meter_tornado_cases_pickle')
