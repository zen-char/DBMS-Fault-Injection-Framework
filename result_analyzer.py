"""
Author: Gerard Schroder
Study:  Computer Science at the University of Amsterdam
Date:   08-06-2016

Give an quick summary of all results obtained by the test scenario runner of a
particular database.

FILE: result_analyzer.py

USAGE: python result_analyzer.py <db_name> [Opt:run_id_UUID]

NOTE: this file is not really flexible and not really neatly written.
      so if errors occur you probably can debug it yourself.

"""
from src.store_results_local import LocalDB
from src.utils import get_time_from_str, get_time_difference
import sys
import uuid


def summarize_results(results, db_type='cassandra'):
    result_count = 0
    query_stats = []
    total = {}
    error_name_list = []
    if db_type == 'cassandra':
        error_name_list = ["read_failure", "time_out", "coordinator_failure",
                           "write_failure", "invalid_request", "no_host_available"]
    for res in results:
        if 'effects' in res:
            query_stat = {}
            test_scenario = res['test_scenario']
            error_sum = 0
            if len(res['effects']) != 0:
                found_db_error = False
                # Example given: 'effects' :  {'1' : {'error1' : 0, 'error2' : 1}, ...}
                for index, query_error in res['effects'].items():
                    if isinstance(query_error, int):
                        print query_error
                        continue
                    else:
                        temp_sum = 0
                        for (key, val) in query_error.items():
                            if key == 'timestamp':
                                continue
                            add_item(query_stat, key, val)
                            error_sum += abs(val)
                            temp_sum += abs(val)
                            if val != 0 and key in error_name_list:
                                found_db_error = True
                        # A query fault found, save the time stamp.
                        if temp_sum != 0:
                            add_item(query_stat, 'fault_time', [query_error['timestamp']])
                            print index, query_error
                if found_db_error:
                    add_item(total, 'error_sum_detected_db', 1)

            print res['res_id']
            query_stat['db_error_logs'] = test_scenario['db_error_logs']
            query_stat['targets'] = test_scenario['target_files']
            query_stat['flips_per_file'] = test_scenario['flips_per_file']
            query_stat['error_sum'] = error_sum
            query_stat['injection_times'] = test_scenario['injection_times']

            latest_injection_time = None
            if len(query_stat['injection_times']) > 0:
                latest_injection_time = get_time_from_str(query_stat['injection_times'][-1])

            if len(query_stat['db_error_logs']) > 0:
                add_item(total, 'error_sum_detected_log', 1)

            if len(query_stat['db_error_logs']) > 0 and latest_injection_time is not None:
                first_log_time = extract_log_time(query_stat['db_error_logs'][0])
                key = 'time_diff_last_injection_log_error'
                query_stat[key] = get_time_difference(latest_injection_time, first_log_time)

            if 'fault_time' in query_stat and latest_injection_time is not None:
                query_stat = get_smallest_time_differences(query_stat)

            print "=== Start Result: {} ===".format(result_count)
            for (key, val) in query_stat.items():
                if val == 0 or (isinstance(val, list) and len(val) == 0):
                    continue
                if key == 'db_error_logs':
                    print key, '- num logs:', len(val), '- values :', normalize_db_logs(val)
                else:
                    print key, ':', val
                if key == 'flips_per_file':
                    data = key + '_n=' + str(val)
                    add_item(total, data, 1)
                elif isinstance(val, int):
                    add_item(total, key, val)
                elif isinstance(val, list):  # and key != 'targets':
                    add_item(total, key, val)

            if error_sum == 0:
                add_item(total, 'no_errors', 1)
            else:
                add_item(total, 'errors', 1)

            print "=== End   Result: {} ===\n".format(result_count)
            query_stats.append(query_stat)
        result_count += 1

    total['total_results'] = result_count
    print
    print "=== Total results: {} ===".format(result_count)
    for (key, val) in total.items():
        if key in ['targets', 'injection_times']:
            continue
            # print
            # print key, ':', len(val), ':', list(set(val))
            # print
        if key == 'time_diff_fault_time_and_injections':
            print key, ':', val
            print 'avg time diff:', calc_avg_time(val)
        if key == 'db_error_logs':
            print key, ':',
            for log in normalize_db_logs(val):
                print log,
        else:
            print key, ':', val
    print "==="
    return query_stats, total


def get_smallest_time_differences(query_stat):
    key = 'time_diff_fault_time_and_injections'
    injection_times = []
    for injection_time in query_stat['injection_times']:
        injection_times.append(get_time_from_str(injection_time))
    fault_times = []
    for fault_time in query_stat['fault_time']:
        fault_times.append(get_time_from_str(fault_time))
    for fault_time in fault_times:
        smallest_time = None
        for injection_time in injection_times:
            diff_time = get_time_difference(fault_time, injection_time)
            if smallest_time is None or diff_time < smallest_time:
                smallest_time = diff_time
        add_item(query_stat, key, [str(smallest_time)])
    return query_stat


def add_item(dictionary, key, val):
    if key in dictionary:
        dictionary[key] += val
    else:
        dictionary[key] = val


def normalize_db_logs(db_logs):
    unique_logs = set()
    for log in db_logs:
        unique_logs.add(log.split(None, 2)[2])
    return list(unique_logs)


# Extract a datetime.time object from a db log in the form:
# e.g. 'WARN 23:30:20 ....'
def extract_log_time(db_log):
    return get_time_from_str(db_log.split(' ', 3)[2])


def calc_avg_time(time_list):
    time_sum_secs = 0
    for time_stamp in time_list:
        time_sum_secs += time_to_secs(time_stamp)
    return time_sum_secs / float(len(time_list))


def time_to_secs(time_stamp):
    split_time = time_stamp.split(":")
    return int(split_time[0]) * (60 * 60) + int(split_time[1]) * 60 + float(split_time[2])


if __name__ == '__main__':
    session = LocalDB(sys.argv[1])
    # When two arguments are given, filter on results from a single experiment.
    if len(sys.argv) == 3:
        test_results = session.query_db({"res_id": uuid.UUID(sys.argv[2])})
    # Else analyse all results.
    else:
        test_results = session.query_db()
    summarize_results(test_results)
