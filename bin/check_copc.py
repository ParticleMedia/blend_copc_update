import sys

class ParamObj:
    def __init__(self, input_file):
        self.param_list = []
        self.param_dict = dict()
        self.is_valid = False
        self.initParam(input_file)

    def initParam(self, input_file):
        f = open(input_file, 'r')
        HEAD_PREFIX = "parameterList="
        head = f.readline().strip()
        if not head.startswith(HEAD_PREFIX):
            self.is_valid = False
            return
        head = head[len(HEAD_PREFIX):]
        self.param_list = head.split(",")

        # read dict
        for line in f.readlines():
            line = line.strip()
            elems = line.split("=")
            if len(elems) != 2:
                self.is_valid = False
                return
            name = elems[0]
            value = 0.0
            try:
                value = float(elems[1])
            except ValueError:
                self.is_valid = False
                return
            self.param_dict[name] = value

        for param in self.param_list:
            if param not in self.param_dict:
                self.is_valid = False
                return
        
        self.is_valid= True
        return


    def checkParamSame(self, new_obj):
        for param in self.param_list:
            if param not in new_obj.param_dict:
                return False
        for param in new_obj.param_list:
            if param not in self.param_dict:
                return False
        return True


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("proc.py org_param_file new_param_file")
        sys.exit(1)

    org_param_obj = ParamObj(sys.argv[1])
    new_param_obj = ParamObj(sys.argv[2])

    if org_param_obj.is_valid == False or new_param_obj.is_valid == False:
        sys.exit(2)

    if not org_param_obj.checkParamSame(new_param_obj):
        sys.exit(1)
