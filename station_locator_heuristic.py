import concurrent.futures
import copy
import csv
import json
import math
import time
import warnings
from collections import defaultdict, Counter
import random

import lazy_table as lt
from tqdm import tqdm

from utilities import euclidean, datetime_for_filename, automkdir, parfile_reader
from datetime import datetime


def station_locator_heuristic(ALL_stations,
                              tornado_cases,
                              pars,
                              heuristic_pars=None,
                              INITIAL_solution=None):
    if INITIAL_solution is None:
        INITIAL_solution = copy.deepcopy(ALL_stations)
    if heuristic_pars is None:
        heuristic_pars = dict(
            time_limit=None,
            iterations_without_improvement=1_000,
        )

    if True:  # Mandatory Pars
        depot_failure_rate = pars["depot_failure_rate"]
        t_bar_maximum = pars["maximum_service_time_hours"] * 60 * 60 * pars["drone_speed_mps"]
        endurance = pars["endurance"] * 60 * 60 * pars["drone_speed_mps"]
        num_tests_each_date = pars["num_tests_each_date"]
        scanning_d = pars['scanning_r'] * 2
        target_feasibility_pct = pars["target_feasibility_pct"]
        perturbation_weights_file = pars["neighborhood_weights_file"]
        perturbation_weights = parfile_reader(perturbation_weights_file)


    station_min_matrix_for_speedup = {date: sorted(
            [
                (min(euclidean(i, ALL_stations[j])
                     for i in tornado_cases.get_specific_event(date)[1].waypoints), j)
                for j in ALL_stations.keys()
            ]
        )
            for date in tqdm(tornado_cases.dates, desc='station_min_matrix_for_speedup')
        }
    logger = SolutionLogger(pars["name"])
    logger.dump_pars(pars)
    logger.dump_pars(perturbation_weights,name='NeighborhoodWeights' )
    logger.write_log_file(['kounter', 'timestamp', 'time_sec', 'n_routes', "t_bar",
                           "n_stations", "n_waypoints", "date_counter", "date", "AllFeasible"])
    fire_station_counter = Counter()
    fire_station_counter.update(set(ALL_stations.keys()))
    pbar = tqdm()
    BEST_solution = copy.deepcopy(ALL_stations)
    CURRENT_solution = copy.deepcopy(INITIAL_solution)
    feasible_solutions = defaultdict(list)
    __ub = len(ALL_stations)
    perturbation_type = "None"
    all_feasible_counter = []
    __gbl_runs = 0
    new_best = False
    while __gbl_runs < 6:
        stopping_criteria = StoppingCriteria(heuristic_pars)
        stopping_criteria.start_timers()

        tabu_list = TabuClass()

        global_counter = 0

        while stopping_criteria.is_not_reached():
            pbar.set_postfix_str(
                f"GBL={str(__gbl_runs)}|"
                f"N={str(global_counter)}|"
                f"CUR={len(CURRENT_solution)}|"
                f"BST={len(BEST_solution)}|"
                f"SC={str(stopping_criteria)}|"
                # f"TL={len(tabu_list)}|"
                f"PT={perturbation_type[:6:]+perturbation_type[6::2]}|"
                f"F%={round(100 * sum(all_feasible_counter) / max(len(all_feasible_counter), 1),2)}% "
                f"(tgt={round(100 * target_feasibility_pct,2)}%)|"
            )
            pbar.update()
            all_feasible_counter = []
            for _date_idx, _tornado_date in enumerate(tornado_cases.dates[::]):
                tornado_date, tornado_event = tornado_cases.get_specific_event(_tornado_date)
                # print(
                #     f"Date: {tornado_date} "
                #       f"(T={int(stopping_criteria.get_time())})"
                #       f"(CUR={len(CURRENT_solution)})"
                #       f"(BST={len(BEST_solution)})"
                #       f"(N={global_counter})"
                #       f"(GBL={__gbl_runs})"
                # )
                for _ in range(num_tests_each_date):
                    up_stations = get_up_stations(CURRENT_solution, depot_failure_rate)
                    t_bar, used_station_keys = super_simple_solution_maker(tornado_event, scanning_d, t_bar_maximum,
                                                                           up_stations, depot_failure_rate,
                                                                           station_min_matrix_for_speedup[
                                                                               _tornado_date])
                    # result_info = super_simple_solution_maker_mp(tornado_event, scanning_d, t_bar_maximum, num_tests_each_date,
                    #                                              CURRENT_solution, depot_failure_rate)
                    # for t_bar, used_station_keys in result_info:
                    global_counter += 1
                    all_feasible_counter.append(t_bar <= t_bar_maximum)
                    logger.write_log_file(
                        [global_counter, datetime.now().strftime('%Y%m%d_%H%M%S'),
                         stopping_criteria.get_time(), len(used_station_keys), t_bar, len(CURRENT_solution),
                         len(tornado_event.waypoints), _date_idx, _tornado_date, bool(t_bar <= t_bar_maximum)]
                    )
                    fire_station_counter.update(tuple(used_station_keys))
            if sum(all_feasible_counter) / max(len(all_feasible_counter), 1) >= target_feasibility_pct:  # all(all_feasible_counter):
                feasible_solutions[len(CURRENT_solution)].append(CURRENT_solution)
                _perturbation_weights = perturbation_weights['feasible']
                #     {
                #     "removal_random": 50,
                #     'removal_weighted_most_often': 10,
                #     'removal_weighted_least_often': 150,
                #     'swap_random': 0,
                #     'swap_intelligent': 50,
                #     'add': 1
                # }
                if len(CURRENT_solution) < len(BEST_solution):
                    new_best = True
                if len(CURRENT_solution) <= len(BEST_solution):
                    BEST_solution = copy.deepcopy(CURRENT_solution)
                    logger.write_solution_file(BEST_solution,
                                               f"{global_counter:0>12}_{int(stopping_criteria.get_time()):0>12}")
            else:
                stopping_criteria.update_iterations_without_improvement_counter()
                _perturbation_weights = perturbation_weights['infeasible']
                #     {
                #     "removal_random": 0,
                #     'removal_weighted_most_often': 0,
                #     'removal_weighted_least_often': 0,
                #     'swap_random': 0,
                #     'swap_intelligent': 9,
                #     'add': 1
                # }
            if __gbl_runs >= 3 and len(CURRENT_solution) >= __ub + 10:
                _perturbation_weights.update(perturbation_weights['late_stage_overrides'])

            if len(CURRENT_solution) / len(BEST_solution) >= 1.25:
                tabu_list.cull_tabu(len(BEST_solution) + 3)
                rando_feas_solution = None
                CURRENT_solution = copy.deepcopy(BEST_solution)

            tabu_list.add_to_tabu(CURRENT_solution.keys())
            CURRENT_solution, perturbation_type = perturb_solution(CURRENT_solution, ALL_stations,
                                                                   fire_station_counter, tabu_info=tabu_list,
                                                                   perturbation_weights=_perturbation_weights)

        CURRENT_solution = copy.deepcopy(BEST_solution)
        __ub = len(BEST_solution)
        __gbl_runs +=1
        if new_best and __gbl_runs >= 3:
            __gbl_runs -= 1
        new_best = False
    logger.solution_flag(len(BEST_solution))
    return BEST_solution


def super_simple_solution_maker_mp_wrapper(args):
    return super_simple_solution_maker(*args)


def super_simple_solution_maker_mp(tornado_event, scanning_d, max_t_bar, num_tests_each_date,
                                   CURRENT_solution, depot_failure_rate):
    result_info = []
    # with concurrent.futures.ProcessPoolExecutor(max_workers=2) as executor:
    with concurrent.futures.ProcessPoolExecutor() as executor:
        results = [executor.submit(
            super_simple_solution_maker_mp_wrapper,
            (tornado_event, scanning_d, max_t_bar, CURRENT_solution, depot_failure_rate)
        ) for _ in range(num_tests_each_date)]
        for result in concurrent.futures.as_completed(results):
            result_info.append(result.result())
    return result_info


def super_simple_solution_maker(tornado_event, scanning_d, max_t_bar,
                                CURRENT_solution, depot_failure_rate, station_min_matrix_for_speedup):
    up_stations = get_up_stations(CURRENT_solution, depot_failure_rate)
    waypoints = tornado_event.waypoints
    num_wpts = len(waypoints)
    approx_len = scanning_d * num_wpts
    n_stations_needed = math.ceil(approx_len / max_t_bar)
    feasible = False
    t_bar, used_station_keys = None, None
    station_min_matrix = [ele for ele in station_min_matrix_for_speedup if ele[1] in up_stations.keys()]
    # station_min_matrix = sorted(
    #     [(min(euclidean(i, up_stations[j]) for i in waypoints), j) for j in up_stations.keys()])
    while not feasible:
        if n_stations_needed > len(up_stations):
            n_stations_needed = len(up_stations)
            feasible = True
        stations_to_use = station_min_matrix[:n_stations_needed]
        first_leg_dists, used_station_keys = zip(*stations_to_use)
        t_bar = max(first_leg_dists) + (approx_len / n_stations_needed)
        if feasible:
            break
        if t_bar > max_t_bar:
            n_stations_needed += 1
            feasible = False
        else:
            feasible = True
    return t_bar, used_station_keys


def get_up_stations(current_solution, depot_failure_rate):
    return {
        key: value for key, value in current_solution.items()
        if random.random() > depot_failure_rate
    }


def _swap_intelligent_v1(current_solution_to_modify, all_stations, factor=2):
    candidate_stations_to_add = list(set(all_stations.keys()).difference(current_solution_to_modify.keys()))
    choices_to_add = [
        (
            pow(min(euclidean(pt, all_stations[i]) for j, pt in current_solution_to_modify.items()), factor),
            i
        ) for i in candidate_stations_to_add
    ]
    choices_to_remove = [
        (
            pow(10, 10) * (1 / pow(
                min(euclidean(pt1, pt2) for j, pt2 in current_solution_to_modify.items() if i != j),
                factor)),
            i
        ) for i, pt1 in current_solution_to_modify.items()
    ]

    wts, chx = zip(*choices_to_add)
    to_add = random.choices(chx, wts)[0]
    wts, chx = zip(*choices_to_remove)
    to_remove = random.choices(chx, wts)[0]
    current_solution_to_modify.pop(to_remove, None)
    current_solution_to_modify[to_add] = all_stations[to_add]
    return current_solution_to_modify

def _solution_perturber(perturbation_weights, current_solution_to_modify, fire_station_counter, all_stations):
    i, j = zip(*perturbation_weights.items())
    perturbation = random.choices(i, j, k=1)[-1]
    if perturbation in {'removal_random'}:
        temp_popper_var = current_solution_to_modify.pop(random.choice(list(current_solution_to_modify.keys())))
    if perturbation in {'removal_weighted_most_often'}:
        temp_popper_var = None
        while temp_popper_var is None:
            temp_popper_var = current_solution_to_modify.pop(random.choice(list(fire_station_counter.elements())), None)
    if perturbation in {'removal_weighted_least_often'}:
        _data = fire_station_counter.most_common()
        _data = [(i, 1.0 / j) for i, j in _data]
        i, j = zip(*_data)
        temp_popper_var = None
        while temp_popper_var is None:
            c = random.choices(i, weights=j)[-1]
            temp_popper_var = current_solution_to_modify.pop(c, None)

    all_stations_keys = set(all_stations.keys())
    current_stations = set(current_solution_to_modify.keys())

    if perturbation in {'swap_intelligent'}:
        if len(all_stations_keys) == len(current_stations):
            return current_solution_to_modify
        _swap_intelligent_v1(current_solution_to_modify, all_stations)
    if perturbation in {'swap_random'}:
        if len(all_stations_keys) == len(current_stations):
            return current_solution_to_modify
        to_add = random.choice(list(all_stations_keys.difference(current_stations)))
        current_solution_to_modify.pop(random.choice(list(current_solution_to_modify.keys())))
        current_solution_to_modify[to_add] = all_stations[to_add]
    if perturbation in {'add'}:
        to_add = random.choice(list(all_stations_keys.difference(current_stations)))
        current_solution_to_modify[to_add] = all_stations[to_add]
    return current_solution_to_modify, perturbation


def perturb_solution(current_solution, all_stations,
                     fire_station_counter, tabu_info=None,
                     perturbation_weights=None):
    if tabu_info:
        for _ in range(100):
            current_solution_to_modify = copy.deepcopy(current_solution)
            current_solution_to_modify, perturb_type = _solution_perturber(perturbation_weights,
                                                                           current_solution_to_modify,
                                                                           fire_station_counter, all_stations)
            if tabu_info.is_tabu(current_solution_to_modify.keys()):
                continue
            else:
                return current_solution_to_modify, perturb_type
    else:
        return _solution_perturber(perturbation_weights, current_solution, fire_station_counter, all_stations)


class SolutionLogger:
    def __init__(self, name=''):
        self.datetime_str = datetime_for_filename()
        self.name = name
        self.working_dir = f"./outputs/output_{name}_{self.datetime_str}"
        self.log_file = f"{self.working_dir}/log_{self.datetime_str}.csv"
        automkdir(self.log_file)
        self.printer = SeansCLogger()
        self._rows = []

    def write_log_file(self, row, c_log=True):
        self._rows.append(row)
        for i in range(100):
            try:
                with open(self.log_file, 'a', newline='') as csvfile:
                    csv_writer = csv.writer(csvfile)
                    csv_writer.writerows(self._rows)
                if len(self._rows) > 1:
                    warnings.warn(f"\nSuccess in writing! ({len(self._rows)} attempts)")
                self._rows = []
                return None
            except:
                pass
        warnings.warn(f"\nFailed to write {self.log_file}")
    def solution_flag(self, data=None):
        with open(f"{self.working_dir}/fin_{data}", 'w') as f:
            f.write('')

    def __str__(self):
        return f"SolutionLogger logging to {self.working_dir}"

    def __repr__(self):
        return f"SolutionLogger path: {self.working_dir}"

    pass

    def write_solution_file(self, solution, substr):
        fp = f"{self.working_dir}/solutions/solution_{len(solution):0>5}_{substr}.json"
        automkdir(fp)
        with open(fp, 'w', newline='') as f:
            json.dump(solution, f, indent=4)
        pass

    def dump_pars(self, pars, name='parameters'):
        fp = f"{self.working_dir}/{name}_{self.datetime_str}.json"
        automkdir(fp)
        with open(fp, 'w', newline='') as f:
            json.dump(pars, f, indent=4)
        pass


class StoppingCriteria:
    def __init__(self, heuristic_pars):
        self.time_limit = heuristic_pars.get('time_limit')
        self.iterations_without_improvement = heuristic_pars.get('iterations_without_improvement')

        self.start_time = None
        self.iterations_without_improvement_counter = 0

    def get_time(self):
        return time.time() - self.start_time

    def start_timers(self):
        self.start_time = time.time()

    def update_iterations_without_improvement_counter(self, *, inc=1):
        self.iterations_without_improvement_counter += inc

    def is_not_reached(self):
        return not self.is_reached()

    def is_reached(self):
        if self.time_limit and time.time() >= self.start_time + self.time_limit:
            return True
        if self.iterations_without_improvement and self.iterations_without_improvement_counter >= self.iterations_without_improvement:
            return True
        return False

    def __str__(self):
        return f"StpCri:"\
               f"{self.iterations_without_improvement_counter}/{self.iterations_without_improvement}" \
               f""


class TabuClass:
    def __init__(self):
        self.tabu_list = defaultdict(list)
        self.tabu_counter = 0

    def add_to_tabu(self, solution_keys):
        self.tabu_list[len(solution_keys)].append(tuple(sorted(solution_keys)))
        self.tabu_counter += 1
        if self.tabu_counter > 100:
            self.auto_cull_tabu()
            self.tabu_counter = 0

    def is_tabu(self, solution_keys):
        return tuple(sorted(solution_keys)) in self.tabu_list[len(solution_keys)]

    def cull_tabu(self, delete_greater_than=None):
        if delete_greater_than and isinstance(delete_greater_than, int):
            _keys = list(self.tabu_list.keys())
            for k in _keys.copy():
                if k > delete_greater_than:
                    self.tabu_list.pop(k, None)

    def auto_cull_tabu(self, factor=0.9):
        _keys = self.tabu_list.keys()
        self.cull_tabu(1 + int(math.ceil(max(_keys) * factor)))

    def __len__(self):
        return sum(len(v) for v in self.tabu_list.values())


class SeansCLogger:
    def __init__(self, header=None):
        self.header = header
        self._k = 99
        self.c_log_to_skip = {1, 7}

    def c_log_row(self, row, padnum=12):
        if self.header is None:
            self.header = row
            return None
        if self._k > 19:
            header = [e for i, e in enumerate(self.header) if i not in self.c_log_to_skip]
            print()
            for ele in header:
                print(f"{self._test_to_str(ele): >12}", end=' |')
            print()
            self._k = 0
        for ele in [e for i, e in enumerate(row) if i not in self.c_log_to_skip]:
            print(f"{self._test_to_str(ele): >12}", end=' |')
        print()
        self._k += 1

    def _test_to_str(self, element, i=0):
        if isinstance(element, float):
            return f"{element:.0f}"
        if isinstance(element, int):
            return f"{element}"
        return f"{element}"
