import json
import os
from datetime import datetime


def parfile_reader(parfilename):
    with open(parfilename) as f:
        parameters = json.load(f)
    return parameters


def parfile_writer(parfilename, data):
    automkdir(parfilename)
    with open(parfilename, "w") as f:
        json.dump(data, f, indent=4)


def json_writer(data, filename):
    with open(filename, 'w') as f:
        json.dump(data, f, indent=4)


def datetime_for_filename(filename=None, _dt=datetime.now().strftime('%Y%m%d_%H%M%S')):
    if filename is None:
        return _dt
    fn, fext = os.path.splitext(filename)
    return f"{fn}_{_dt}{fext}"


def automkdir(filename):
    os.makedirs(os.path.dirname(filename), exist_ok=True)


def _convert_df_to_alternative_data_structure(original_df):
    new_struct = {f'{index}_{row["id"]}_{row["name"]}'.replace(' ', '_'):
                      dict(name=row["name"], index=index, point=(row.geometry.x, row.geometry.y), row_id=row["id"])
                  for index, row in original_df.iterrows()}
    return new_struct


def euclidean(p1, p2):
    return pow(sum(pow((a - b), 2) for a, b in zip(p1, p2)), 0.5)


if __name__ == '__main__':
    print(datetime_for_filename('foo.csv'))
