import numpy as np
from collections import OrderedDict

class Model:
    'Common base class for all models'

    def __init__(self, id, model_size, zoi,scenario):
        # print ("Creating new message...")
        self.id = id
        self.size = model_size
        self.zoi = zoi
        self.contributions = OrderedDict()
        self.scenario = scenario
        self.computing_time = self.scenario.computing_time
        self.counter = [0] * scenario.num_zois
        for i in range(self.scenario.num_zois):
            self.counter[i] = 0
        # self.displayModel()


    def displayModel(self):
        print("ID : ", self.id,  ", Size: ", self.size,  ", ZOI: ", self.zoi.id)

    def copy(self):
        copied_model = Model(self.id, self.size, self.zoi, self.scenario)
        for k,v in self.contributions.items():
            copied_model.contributions[k] = v
        return copied_model