import pickle

from utilities import parfile_reader, json_writer


def pickle_input(pickle_file_to_try_to_load):
    print(f"Trying to load pickle file {pickle_file_to_try_to_load}")
    try:
        data = pickle.load(open(pickle_file_to_try_to_load, "rb"))
        print(f" Loaded file {pickle_file_to_try_to_load}")
        return data
    except:
        print(f" Failed to load file {pickle_file_to_try_to_load}")
        return None


def pickle_dumper(filename, data, key, pickleparfile):
    print(f"Dumping Picklefile {filename}")
    pickles = parfile_reader(pickleparfile)
    pickle.dump(data, open(filename, "wb"))
    pickles[key] = filename
    json_writer(pickles, pickleparfile)
