#!/bin/python
import sys


def load_dict(file_name):
    f = open(file_name, 'r')
    (head, param_dict) = ("", {})

    head = f.readline()
    if head == None:
        return ("", {})
    else:
        head = head.strip()

    for line in f.readlines():
        elems = line.strip().split("=")
        param_dict[elems[0]] = elems[1]
    f.close()
    return (head, param_dict)


def update_file(head, param_dict, out_file):
    out_f = open(out_file, 'w')

    out_f.write("{}\n".format(head))
    head_elems = head.strip().split("=")[1].split(",")
    for head_elem in head_elems:
        key = head_elem
        value = param_dict[key]
        out_f.write("{}={}\n".format(key, value))
    out_f.close()


def merge_head(in_head, out_head):
    if len(out_head) == 0:
        out_head = "parameterList="
        return "{}{}".format(out_head, in_head)
    else:
        return "{},{}".format(out_head, in_head)


def merge_dict(dict1, dict2):
    out_dict = {}
    for key, value in dict1.items():
        out_dict[key] = value
    for key, value in dict2.items():
        out_dict[key] = value
    return out_dict


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("proc.py in_param out_param")
        sys.exit(1)

    (in_head, in_dict) = load_dict(sys.argv[1])
    print(in_head)
    print(len(in_dict))
    (out_head, out_dict) = load_dict(sys.argv[2])
    print(out_head)
    print(len(out_dict))

    new_head = merge_head(in_head, out_head)
    new_dict = merge_dict(in_dict, out_dict)
    print(len(new_dict))
    update_file(new_head, new_dict,  sys.argv[2])
