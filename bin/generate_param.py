import sys
from configparser import ConfigParser
from impala_utils import *
from sql_new import *
import pandas as pd
import numpy as np
import math
from datetime import datetime
from datetime import timedelta


def get_prev_pdate(pdate_str, day_num):
    pdate = datetime.strptime(pdate_str, "%Y-%m-%d")
    prev_pdate = pdate - timedelta(days = day_num)
    prev_pdate_str = prev_pdate.strftime("%Y-%m-%d")
    return prev_pdate_str


def list_to_df(feature_list, docs):
    data = pd.DataFrame(docs)
    name_dict = {}
    for i in range(0, len(feature_list)):
        name_dict[i] = feature_list[i]
        data.rename(columns = name_dict, inplace = True)
    return data


def deal_org_score(docs):
    feature_list = ["os", "score"]
    data = list_to_df(feature_list, docs)
    x = data["score"]
    median = x.quantile(q = 0.5)

    Q1 = x.quantile(q = 0.25)
    Q3 = x.quantile(q = 0.75)

    low_bound = Q1 - 1.5*(Q3 - Q1)
    up_bound = Q3 + 1.5*(Q3 - Q1)
    x = x[(x >= low_bound) & (x <= up_bound)]
    np_x = np.array(x)
    x0 = np.mean(np_x)
    var_x = np.var(np_x)
    k = 1.0 / math.sqrt(var_x + 0.00001)
    print("median={}, mean={}, var={}, k={}, low_bound={}, up_bound={}".format(median, x0, var_x, k, low_bound, up_bound))
    return (median, k)


def init_config(os_list, user_condition_list, doc_condition_key, exp_name):
    head = ""
    config_dict = {}
    for os_sql, os_key in os_list:
        for user_condition_sql, user_condition_key in user_condition_list:
            for param_key in ["x0", "k", "copc"]:
                key = "{}_{}_{}_{}".format(os_key, user_condition_key, doc_condition_key, param_key)
                if len(exp_name) != 0:
                    key = "{}_{}_{}_{}_{}".format(exp_name, os_key, user_condition_key, doc_condition_key, param_key)
                config_dict[key] = 0.0
                if len(head) == 0:
                    head = "{}".format(key)
                else:
                    head = "{},{}".format(head, key)
    return (head, config_dict)


def output_config(head, config_dict, out_p):
    out_f = open(out_p, 'w')
    out_f.write("{}\n".format(head))

    head_elems = head.split(",")

    for key in head_elems:
        out_f.write("{}={}\n".format(key, config_dict[key]))
    out_f.close()


def update_dict(os, user_condition, doc_condition, x0, k, docs, config_dict, exp_name):
    msg = ""
    if len(docs) != 1 and len(docs[0]) != 2:
        print("error: res")
        for doc in docs:
            print(",".join([str(i) for i in doc]))
        return

    copc = docs[0][0]
    auc = docs[0][1]

    x0_key = "{}_{}_{}_x0".format(os, user_condition, doc_condition)
    k_key = "{}_{}_{}_k".format(os, user_condition, doc_condition)
    copc_key = "{}_{}_{}_copc".format(os, user_condition, doc_condition)

    if len(exp_name) != 0:
        x0_key = "{}_{}_{}_{}_x0".format(exp_name, os, user_condition, doc_condition)
        k_key = "{}_{}_{}_{}_k".format(exp_name, os, user_condition, doc_condition)
        copc_key = "{}_{}_{}_{}_copc".format(exp_name, os, user_condition, doc_condition)

    if x0_key in config_dict:
        msg = "{} key={}, old_value={}, new_value={}\n".format(msg, x0_key, config_dict[x0_key], x0)
        config_dict[x0_key] = x0
    else:
        raise Exception("key={}, value={} not in configDict".format(x0_key, x0))

    if k_key in config_dict:
        msg = "{} key={}, old_value={}, new_value={}\n".format(msg, k_key, config_dict[k_key], k)
        config_dict[k_key] = k
    else:
        raise Exception("key={}, value={} not in configDict".format(k_key, k))

    if copc_key in config_dict:
        msg = "{} key={}, old_value={}, new_value={}\n".format(msg, copc_key, config_dict[copc_key], copc)
        config_dict[copc_key] = copc
    else:
        raise Exception("key={}, value={} not in configDict".format(copc_key, copc))
    return msg


def main_group(prev_pdate, curr_pdate, exp_name, local_tag, out_p):
    doc_condition_sql, doc_condition_key = ('cjv.nr_condition = "local"', "local")
    if local_tag == "nonlocal":
        doc_condition_sql, doc_condition_key = ('cjv.nr_condition not like "%local%"', "nonlocal")

    os_list = [("android", "and"), ("ios", "ios")]
    user_condition_list = [('localG', "localG"), ('nonlocalG', "nonlocalG"), ('commonG', 'commonG'), ('unbeliefG', 'unbeliefG') ]

    msg = ""
    (head, config_dict) = init_config(os_list, user_condition_list, doc_condition_key, exp_name)

    for os_sql, os_key in os_list:
        for user_condition_sql, user_condition_key in user_condition_list:
            sql = dump_score_sql_group(os_sql, prev_pdate, curr_pdate, user_condition_sql, doc_condition_sql, exp_name)
            print(sql)
            docs = execute_hue_with_retry("lixingni_tmp_job", sql, 5)
            (x0, k) = deal_org_score(docs)
            print("os={}, user_condition={}, doc_condition={}, x0 = {}, k= {}".format(os_key, user_condition_key, doc_condition_key, x0, k))
            sql = calc_copc_sql_group(os_sql, prev_pdate, curr_pdate, user_condition_sql, doc_condition_sql, x0, k, exp_name)
            print(sql)
            docs = execute_hue_with_retry("lixingni_tmp_job", sql, 5)
            for doc in docs:
                print(",".join([str(i) for i in doc]))
            msg += update_dict(os_key, user_condition_key, doc_condition_key, x0, k, docs, config_dict, exp_name)
    output_config(head, config_dict, out_p)
    return msg


if __name__ == "__main__":
    if len(sys.argv) != 5:
        print("proc.py curr_pdate expName [local/nonlocal] out.file")
        sys.exit(1)
    curr_pdate = sys.argv[1]
    prev_pdate = get_prev_pdate(curr_pdate, 11)
    exp_name = sys.argv[2]
    local_tag = sys.argv[3]
    out_file = sys.argv[4]
    msg = main_group(prev_pdate, curr_pdate, exp_name, local_tag, out_file)
    print(msg)
