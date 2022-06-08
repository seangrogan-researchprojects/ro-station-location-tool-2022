from file_readers import read_waypoint_file, read_fire_stations_file, \
    tornado_cases_reader
from multi_knapsack_min_packing_solver_for_prj_william import multi_knapsack_min_packing_covering_solver
from station_locator_heuristic import station_locator_heuristic
from utilities import parfile_reader, datetime_for_filename


def main(parfile='./par_files/par1.json',
         pickles='./pickles/pickles.json',
         pickle_case='300_meter_tornado_cases_pickle'):
    pars = parfile_reader(parfile)
    pickle_files = parfile_reader(pickles)

    output_folder = f"./{pars['name']}_{datetime_for_filename()}/"

    fire_stations = read_fire_stations_file(pars['fire_stations'], pars['crs'])
    waypoints = read_waypoint_file(pars['waypoints_file'])

    tornado_cases = tornado_cases_reader(pars, pickle_files, pickle_case, pickles, waypoints)

    solution_keys, all_stations = multi_knapsack_min_packing_covering_solver(
        read_waypoint_file(pars['waypoints_file_for_heuristic']), fire_stations,
        pars['maximum_service_time_hours'], pars, exact=False)
    initial_solution = {k:v for k, v in all_stations.items() if k in solution_keys}
    station_locator_heuristic(
        all_stations,
        tornado_cases,
        pars,
        heuristic_pars=None,
        INITIAL_solution=initial_solution
    )
    return 0


if __name__ == '__main__':
    main(parfile='./par_files/PrioritizeRandomSwaps/par1.json',
         pickles='./pickles/pickles.json',
         pickle_case='300_meter_tornado_cases_pickle')
