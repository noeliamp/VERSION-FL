
class LandMark:
    'Common base class for all Land Marks'

    def __init__(self, id, posX, posY, scenario, zoi,radius):
        # print ("Creating new ZOI...")
        self.id = id
        self.scenario = scenario
        self.zoi = zoi
        self.x = posX
        self.y = posY
        self.radius = radius
        self.square_radius = radius*radius
        # self.displayLandMark()


    def displayLandMark(self):
        print("ID : ", self.id,  ", POS X: ", self.x,  ", POS Y: ", self.y)