import copy

import pulp
from scipy import spatial
from tqdm import tqdm, trange
import numpy as np

from file_readers import read_fire_stations_file, read_waypoint_file
from utilities import _convert_df_to_alternative_data_structure, parfile_reader


def multi_knapsack_min_packing_covering_solver(waypoints, fire_stations, max_service_time_hr, pars, exact=False):
    max_service_time = max_service_time_hr * 60 * 60 * pars["drone_speed_mps"]
    stations = _convert_df_to_alternative_data_structure(fire_stations)
    stations = {k: v['point'] for k, v in stations.items()}
    station_keys, station_points = zip(*stations.items())
    waypoints = list(waypoints)
    d_matrix = spatial.distance_matrix(x=station_points, y=waypoints)
    solution = multi_knapsack_min_packing_covering_heuristic(d_matrix, len(stations),
                                                             len(waypoints), max_service_time)
    if exact:
        solution = multi_knapsack_min_packing_covering_exact(station_keys, waypoints,
                                                             d_matrix, max_service_time,
                                                             warm_starter=solution)
    else:
        solution = {station_keys[idx] for idx in solution}
    return solution, stations


def multi_knapsack_min_packing_covering_heuristic(d_matrix, n_depots, n_wp, max_service_time):
    solution = dict()
    d_matrix_copy = copy.deepcopy(d_matrix)
    depots = list(range(n_depots))
    wp = set(range(n_wp))
    p_bar = tqdm(
        desc="Finding Heuristic Solution", postfix=dict(N_Depot=len(solution), N_WP=len(wp)))
    while len(wp) > 0:
        _depot, _wp = np.where(d_matrix_copy == np.amin(d_matrix_copy))
        _depot, _wp = int(_depot[0]), int(_wp[0])
        solution[_depot] = [_wp]
        wp.discard(_wp)
        d_matrix_copy[:, _wp] = np.inf
        dists_from_depot = d_matrix_copy[_depot]
        while sum(d_matrix[_depot, j] for j in solution[_depot]) < max_service_time and len(wp) > 0:
            _wp = np.where(dists_from_depot == np.amin(dists_from_depot))
            _wp = int(_wp[0])
            if _wp == 102:
                print()
            if _wp not in wp:
                dists_from_depot[_wp] = np.inf
                d_matrix_copy[_depot, _wp] = np.inf
                continue
            if sum(d_matrix[_depot, j] for j in solution[_depot]) + d_matrix[_depot, _wp] > max_service_time:
                break
            solution[_depot].append(_wp)
            dists_from_depot[_wp] = np.inf
            d_matrix_copy[:, _wp] = np.inf
            wp.discard(_wp)
        p_bar.set_postfix(dict(N_Depot=len(solution), N_WP=len(wp)))
        p_bar.update()
    return solution


def multi_knapsack_min_packing_covering_exact(station_keys, waypoints, d_matrix, ub_service_time,
                                              warm_starter=None):
    print(f"Building PuLP model to solve a min covering set")
    problem = pulp.LpProblem(f"MinCoveringWithPulp", pulp.LpMinimize)
    y = [pulp.LpVariable(f"y_{i}", 0, 1, pulp.LpInteger) for i in trange(len(station_keys), desc='y vars')]
    x = {i: {j: pulp.LpVariable(f"x_{i}_{j}", 0, 1, pulp.LpInteger) for j in range(len(waypoints))
             # if d_matrix[i, j] <= ub_service_time
             } for i in
         trange(len(station_keys), desc='x vars')}
    # e = {j: pulp.LpVariable(f"e_{j}", 0, 0) for j, wp in tqdm(enumerate(waypoints))}
    problem += pulp.lpSum(y[i] for i in trange(len(station_keys), desc='ObjFn')), "ObjFn"
    # M = len(waypoints)
    for j in trange(len(waypoints), desc='Visit Constraint'):
        problem += pulp.lpSum(x[i][j] for i in range(len(station_keys))) == 1, f"c_wp_{j}"
        # [x[i][j].setInitalValue(0) for i in range(len(station_keys))]
    for i in trange(len(station_keys), desc='Limit Constraint'):
        # for j, wp in enumerate(waypoints):
        #     problem += x[i][j] * d_matrix[i, j] <= ub_service_time
        # problem += pulp.lpSum(x[i][j] for j in range(len(waypoints))) - M * y[i] <= 0
        # problem += pulp.lpSum(x[i][j] * d_matrix[i, j] for j in range(len(waypoints))) <= ub_service_time
        problem += pulp.lpSum(x[i][j] * d_matrix[i, j] for j in range(len(waypoints))) - ub_service_time * y[
            i] <= 0, f"c_dp_{i}"
    print(f"trying to solve...")
    # problem.solve(pulp.PULP_CBC_CMD(msg=1))
    if warm_starter:
        print("warm starting...")
        for y_id, y_var in enumerate(y):
            if y_id in warm_starter.keys():
                y[y_id].setInitialValue(1)
                for _wp in warm_starter[y_id]:
                    x[y_id][_wp].setInitialValue(1)
            else:
                y[y_id].setInitialValue(0)
    problem.solve(
        # pulp.GUROBI_CMD(msg=1, warmStart=1, options=[('TimeLimit', 180), ('MIPGap', 0.33), ("MIPFocus", 3)])
        pulp.GUROBI_CMD(msg=1, warmStart=1, options=[("MIPFocus", 2)])
    )
    # print()
    solution_keys = {station_keys[idx] for idx, var in enumerate(y) if var.varValue > 0.9}
    return solution_keys


if __name__ == '__main__':
    #exact vs other tests
    pars = parfile_reader("./par_files/par1.json")
    fire_stations = read_fire_stations_file(pars['fire_stations'], pars['crs'])
    waypoints = read_waypoint_file(pars['waypoints_file_for_heuristic'])
    print("Heuristic")
    solution, stations= multi_knapsack_min_packing_covering_solver(waypoints, fire_stations, 5, pars, exact=False)
    print (len(solution))
    print("Exact")
    solution, stations = multi_knapsack_min_packing_covering_solver(waypoints, fire_stations, 5, pars, exact=True)
    print(len(solution))