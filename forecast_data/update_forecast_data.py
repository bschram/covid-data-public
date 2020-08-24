import covidcast
from multiprocessing import Pool


def get_data(signal, datasource):
    print(signal, datasource)
    data = covidcast.signal(datasource, signal, None, None, "state")
    data.to_csv(f"{signal}.csv")


if __name__ == "__main__":
    # signals = {"full_time_work_prop": "safegraph", "part_time_work_prop":"safegraph", "median_home_dwell_time": "safegraph", "completely_home_prop": "safegraph","smoothed_cli": "doctor-visits", "smoothed_adj_cli": "doctor-visits"}
    signals = {"median_home_dwell_time": "safegraph"}
    for signal, datasource in signals.items():
        get_data(signal, datasource)

    print("saved data")
    # with Pool(processes = 4) as p:
    #  p.map(get_data(signals))
