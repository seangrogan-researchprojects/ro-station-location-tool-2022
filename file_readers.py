import pandas as pd
import geopandas as gpd
from shapely.geometry import LineString

from TornadoCaseGetter import TornadoCaseGetter
from pickles.pickle_io import pickle_input, pickle_dumper


def read_fire_stations_file(fire_stations_file, crs):
    if isinstance(fire_stations_file, list):
        data = []
        for file in fire_stations_file:
            data.append(read_fire_stations_file(file, crs))
        return pd.concat(data)
    # print(f"Reading : {fire_stations_file}")
    fire_stations = _read_geo_files_into_geopandas(fire_stations_file, crs)
    fire_stations = fire_stations[~(fire_stations['id'].isnull())]
    return fire_stations

def read_waypoint_file(waypoint_file):
    if isinstance(waypoint_file, list):
        data = []
        for file in waypoint_file:
            data.extend(read_waypoint_file(file))
        return data
    print(f"Reading : {waypoint_file}")
    waypoints = pd.read_csv(waypoint_file)
    waypoints = list(tuple(x) for x in waypoints.to_records(index=False))
    return waypoints


def read_tornadoes(tornado_file, crs, keep_unknown_ends=True):
    if isinstance(tornado_file, list):
        data = []
        for file in tornado_file:
            data.append(read_tornadoes(file, crs, keep_unknown_ends))
        return pd.concat(data)
    print(f"Reading : {tornado_file}")
    tornado_db = pd.read_csv(tornado_file)
    if keep_unknown_ends:
        tornado_db.elon = tornado_db.apply(lambda x: x.slon + 0.001, axis=1)
        tornado_db.elat = tornado_db.apply(lambda x: x.slat + 0.001, axis=1)
    else:
        tornado_db = tornado_db[~((tornado_db['elon'] == 0) & (tornado_db['elat'] == 0))]
    geometries = [LineString([(x0, y0), (x1, y1)])
                  for x0, y0, x1, y1
                  in zip(tornado_db.slon, tornado_db.slat, tornado_db.elon, tornado_db.elat)]
    tornado_db = gpd.GeoDataFrame(tornado_db, crs="EPSG:4326", geometry=geometries)
    tornado_db = tornado_db.to_crs(crs=crs)
    tornado_db['datetime'] = tornado_db['date'].str.cat(tornado_db['time'], sep=" ")
    tornado_db = _geopandas_fix_datetime(tornado_db, cols=['datetime'], fmt='%Y-%m-%d %H:%M:%S')
    tornado_db = _geopandas_fix_datetime(tornado_db, cols=['date'], fmt='%Y-%m-%d')
    tornado_db['date'] = tornado_db['date'].dt.date
    tornado_db = _geopandas_fix_datetime(tornado_db, cols=['time'], fmt='%H:%M:%S')
    tornado_db['time'] = tornado_db['time'].dt.time
    return tornado_db


def read_sbw_file(sbws, crs):
    sbws = _read_geo_files_into_geopandas(sbws, crs)
    sbws = sbws[sbws['GTYPE'] == 'P']
    sbws = _geopandas_fix_datetime(sbws,
                                   cols=['ISSUED', 'EXPIRED', 'INIT_ISS', 'INIT_EXP'],
                                   fmt='%Y%m%d%H%M%S')
    return sbws


def _geopandas_fix_datetime(gdf, cols=None, fmt='%Y%m%d%H%M%S'):
    """Adds date time to :param cols: in a :param gdf: geopandas dataframe.  :param fmt: is the datetime format"""
    if isinstance(cols, str):
        gdf[cols] = pd.to_datetime(gdf[cols], format=fmt)
    else:
        for col in cols:
            gdf[col] = pd.to_datetime(gdf[col], format=fmt)
    return gdf


def _read_geo_files_into_geopandas(files, crs="EPSG:4326"):
    if isinstance(files, str):
        print(f"Reading : {files}")
        gp_df = gpd.read_file(files)
        gp_df = gp_df.to_crs(crs=crs)
        return gp_df
    gp_df = []
    for file in files:
        print(f"Reading : {file}")
        gp_df.append(gpd.read_file(file))
        gp_df[-1] = gp_df[-1].to_crs(crs=crs)
    gp_df = pd.concat(gp_df)
    return gp_df


def tornado_cases_reader(pars, pickle_files, pickle_case, pickles, waypoints):
    tornado_cases = pickle_input(pickle_files[pickle_case])

    if tornado_cases is None:
        tornado_db = read_tornadoes(pars['tornado_db'], pars['crs'], keep_unknown_ends=True)
        sbws = read_sbw_file(pars['sbws'], pars['crs'])
        tornado_cases = TornadoCaseGetter(tornado_db, sbws, waypoints)
        pickle_dumper(f"./pickles/{pickle_case}.pkl", tornado_cases, pickle_case, pickles)
    return tornado_cases