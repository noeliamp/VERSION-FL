
class Zoi:
    'Common base class for all ZOIS'

    def __init__(self, id, posX, posY, scenario):
        # print ("Creating new ZOI...")
        self.id = id
        self.scenario = scenario
        self.x = posX
        self.y = posY
        self.model_list = []
        # self.displayZoi()


    def displayZoi(self):
        print("ID : ", self.id,  ", POS X: ", self.x,  ", POS Y: ", self.y, ", Models list length: ", len(self.models_list))