import numpy as np
import math 
from collections import Counter 
from collections import OrderedDict
import sys
import geopy.distance

class User:
    'Common base class for all users'

    def __init__(self, id, posX, posY, scenario, memory):

        # print ("Creating new user...")

        self.id = id
        self.scenario = scenario
        self.total_memory = memory
        self.model_list = []
        self.pending_model_list = []
        self.computing_counter = 0
        self.merging_counter = 0
        self.exchange_list = []
        self.exchange_size = 0
        self.prev_peer = None
        self.busy = False # busy only per slot
        self.ongoing_conn = False
        self.db_exchange = False
        self.x_list = []
        self.y_list = []
        self.x_list.append(posX)
        self.y_list.append(posY)
        self.isPaused = False
        self.flight_length= np.inf
        self.x2 = 0
        self.y2 = 0
        self.xb = []
        self.yb = []
        self.xs = 0
        self.xd = 0
        self.ys = 0
        self.yd = 0
        self.first_move = True
        self.new_point_x = 0
        self.new_point_y = 0
        self.speed = (np.random.uniform(self.scenario.max_speed,self.scenario.min_speed))*self.scenario.delta
        self.N12 = np.inf            # slots to reach target position (x2,y2) 
        self.n = 1                   # current slot within N12 for random waypoint
        self.m = 0                   # current slot for random Direction
        self.rebound_counter = 0
        self.neighbours_list = []
        self.counter_list = []
        self.exchange_counter = 0
        self.used_memory = 0
        self.vx = 0
        self.vy = 0
        self.out = False
        self.entering_landmark = [True]*self.scenario.num_landmarks
        self.x_origin = 0
        self.y_origin = 0
        self.connection_duration = 0
        self.contacts_per_slot_dynamic = OrderedDict()
        self.contacts_per_slot_static = OrderedDict()
        self.myFuture = OrderedDict()
        self.existing1 = True
        self.existing2 = True
        self.observations = OrderedDict()
        self.calculateZones(1)
        self.observation_to_train = None
        self.list_to_merge = []
        self.freshness = OrderedDict()
        for i in range(self.scenario.num_landmarks):
            self.freshness[i] = []
        

        # self.displayUser()

    
    def displayUser(self):
        print("ID : ", self.id,  ", Total Memory: ", self.total_memory,  ", Used Memory: ", self.used_memory, ", PosX: ",self.x_list, 
              ", PosY: ", self.y_list, ", Is Paused: ", self.isPaused, ", Slots Paused: ", self.pause_slots, 
              ", Counter Paused: ", self.pause_counter, ", slot n: ", self.n, ", Model list: " , len(self.model_list), ", Coordinates list: " , len(self.x_list))
        for z in self.zones:
            print("ZOI ID: ", z.id)
            print("Zone: ", self.zones[z])

    def getObservations(self,c):
        for lm in self.scenario.landmark_list:
            if self.scenario.city =="Paderborn" or self.scenario.city =="Luxembourg" or self.scenario.city == "none" :
                d = np.power(self.x_list[-1]- lm.x,2) + np.power(self.y_list[-1]- lm.y,2)
            if self.scenario.city !="Paderborn" and self.scenario.city !="Luxembourg" and self.scenario.city != "none" :
                coords_1 = (lm.x, lm.y)
                coords_2 = (self.x_list[-1], self.y_list[-1])
                d = geopy.distance.distance(coords_1, coords_2).m
            
            if d < lm.square_radius and self.entering_landmark[lm.id]:
                self.entering_landmark[lm.id] = False
                if lm.id not in self.observations:
                    self.observations[lm.id] = []
                self.observations[lm.id].append(c)
                observ= tuple((c,lm.id,self.id))
                print(self.id,"entro en landmark y genero una observation", observ)

                # if the node has no model, we give it a new model to start training
                if len(self.pending_model_list) == 0 and len(self.model_list):
                    self.pending_model_list.append(self.scenario.zois_list[0].model_list[0].copy())
                    print(self.id,"no tengo modelos asi que creo uno para", observ)


                if len(self.scenario.observations_mean_rate) == 0:
                    self.scenario.observations_mean_rate.append(c)
                if len(self.scenario.observations_mean_rate) > 0:
                    self.scenario.observations_mean_rate.append(c)

                self.scenario.observations_counter += 1
                if c > 5000 and len(self.scenario.observations_processing_list) < np.inf:
                    self.scenario.observations_processing_list.append(tuple((c,lm.id,self.id)))
                    print("METO UNA NUEVA OBSERVATION---->", tuple((c,lm.id,self.id)))
            
            if d > lm.square_radius:
                self.entering_landmark[lm.id] = True
           
    
    def calculateZones(self,c):
        # print("Slot:", c, "node: ", self.id)
        self.zones = OrderedDict()
        for z in self.scenario.zois_list:
            if self.scenario.city =="Paderborn" or self.scenario.city =="Luxembourg" or self.scenario.city == "none" :
                d = np.power(self.x_list[-1]- z.x,2) + np.power(self.y_list[-1]- z.y,2)
            if self.scenario.city !="Paderborn" and self.scenario.city !="Luxembourg" and self.scenario.city != "none" :
                coords_1 = (z.x, z.y)
                coords_2 = (self.x_list[-1], self.y_list[-1])
                d = geopy.distance.distance(coords_1, coords_2).m
            
            if d <= z.scenario.square_radius_of_replication:
                self.out = False
                # print("replication")
                self.zones[z] = "replication"
                self.myFuture[c] = z.id
                # print("calculating in replication", self.id)
            if d < z.scenario.square_radius_of_interest:
                # print("entro en replication", c, self.newObservation)
                self.out = False
                # print("interest")
                self.zones[z] = "interest"
                self.myFuture[c] = z.id
                # print("calculating in interes", self.id)
            if d > z.scenario.square_radius_of_replication:
                self.out = True
                # if c not in self.myFuture:
                self.myFuture[c] = -1
                if c > 1 and self.myFuture[c-1] != -1:
                    self.scenario.exiting_nodes[c] += 1
                    
                # if len(self.model_list) > 0:
                    # print(self.id,"Going to exit RZ with--->",len(self.model_list[0].contributions),self.model_list[0].contributions.keys())

                # We do not keep information about the zones where the node is out
                # if self.ongoing_conn == False:
                self.deleteModels(z)
                
            # print("zoi:", z, self.zones[z])

        # print("estoy en:",self.myFuture[c])

    # def deleteModels(self,z):
        # We remove the messages belonging to this zone in case the node was previously in the zone (the zone existed before)    \
        # print("My id: ", self.id, " zone: ", z.id)
        # print("Used memory: ", self.used_memory)
        # # print("Exchange list size: ", self.exchange_size)
        # for m in self.model_list:
        #     # print("MESSAGE: ", m.id, " zone: ", m.zoi.id)
        #     if m.zoi == z:
        #         # print("Entro en borrar de mi lista el size: ", m.size, "lista: ", len(self.model_list))
        #         self.used_memory -= m.size
        #         self.model_list.remove(m)
        #         # print("lista: ", len(self.model_list))
        #         if m in self.exchange_list:
        #             # print("Entro en borrar de mi exchange", len(self.exchange_list),self.exchange_size)
        #             self.exchange_size -= m.size
        #             self.exchange_list.remove(m)
        #             # print("despues de borrar exchange", len(self.exchange_list),self.exchange_size)

        # for m in self.pending_model_list:
        #     if m.zoi == z:
        #         self.pending_model_list.remove(m)
        #         self.computing_counter = 0
               
        
        # self.db_exchange = False what to do with this?
        # print("Dropping my DB")
        # print("Used memory: ", self.used_memory)
        # print("Exchange list size: ", self.exchange_size)


    def deleteModels(self,z):
        self.model_list = []
        self.pending_model_list = []
        self.computing_counter = 0
        self.merging_counter = 0
        self.list_to_merge = []
        self.observations = OrderedDict()
    
    def getNextWayPoint(self):
        self.x2 = np.random.uniform(-self.scenario.max_area,self.scenario.max_area)
        self.y2 = np.random.uniform(-self.scenario.max_area,self.scenario.max_area)

    def randomWaypoint(self,c):
  
            # print ("My id is ", self.id)           
            # if it is the first time that the user is moving from origin (x1,y1), 
            # we need to  calculate target position (x2,y2) and the number of slots to reach target position (only at the beginning)
            if self.n==1:
                self.randomWaypointParameters()
   
            # If we are at the beggining of a path between 2 points, 
            # save source and destination coordinates and N12 as self information for the following slots (It is going to be the same until n = N12)
            if self.n == 1:
                # print("Entering in n == 1 for a new rebound")
                self.xs = self.xb[self.rebound_counter]
                self.ys = self.yb[self.rebound_counter]
                self.xd = self.xb[self.rebound_counter + 1]
                self.yd = self.yb[self.rebound_counter + 1]
                # print(self.xs, self.ys, self.xd, self.yd)
                # Distance between 2 points
                dist = np.sqrt(np.power((self.xb[self.rebound_counter+1]-self.xb[self.rebound_counter]),2) + 
                            np.power((self.yb[self.rebound_counter+1]-self.yb[self.rebound_counter]),2))
                # print('Distancia----> ',dist)
                # Time to move from (xb1,yb1) to (xb2,yb2)
                self.T12 = dist/self.speed
                # print('t12----> ',self.T12)

                # Number of slots to reach (xb2,yb2)
                self.N12 = np.ceil((self.T12))
                # print("Number of slots until target/border position N12 --> %d" % self.N12)

            # we need to find the new intermediate position between n and n-1 regarding user speed
            # Euclidean vector ev = (x2-x1,y2-y1) and next position after one time slot (xi,yi) = (x1,y1) + n/N12 * ev
            xi = self.xs + (self.n/self.N12) * (self.xd - self.xs)   
            yi = self.ys + (self.n/self.N12) * (self.yd - self.ys)
            # print("xi --> " , xi)
            # print("yi --> " , yi)
    

            # Once the intermediate position (xi,yi) is selected, add the coordinates to the lists
            self.x_list.append(xi)
            self.y_list.append(yi)

            self.n = self.n + 1

            # if we have reached a bound position, update the counters to start again with the next
            if self.n == self.N12 + 1:
                self.n = 1
                self.N12 = 0
                self.rebound_counter += 1
                # print("rebound_counter: ",self.rebound_counter)
                # print("Bound position reached, n = 0.")
                    

            if self.rebound_counter == len(self.xb)-1:
                self.rebound_counter = 0
                self.xb = []
                self.yb = []
                # print("Final target position reached, rebound counter = 0.")

        
            # # Check the new point zone and print the info of the user
            # self.calculateZones(c)




        # with this method we create two lists xb and yb at the beggining of the mobility with all the border and targer positions of the user
    def randomWaypointParameters(self):
        self.getNextWayPoint()            
        # print("X2: ",self.x2, ", Y2: ", self.y2)
        # Save current position
        self.xb.append(self.x_list[-1]) 
        self.yb.append(self.y_list[-1])

        # Append target possition to the end of the list 
        # (if the target position is never out of bounds this list will only containg the first position and the targer position)
        # print("Final target position ----> X2: ", self.x2, ", Y2: ", self.y2)
            
        self.xb.append(self.x2)
        self.yb.append(self.y2)


    def randomDirection(self,c):
        # initial position m = 1
        if self.first_move:
            # select an angle
            randNum = np.random.uniform()
            alpha = 360 * randNum *(math.pi/180)

            # vector based on angle
            self.vx = math.cos(alpha)
            self.vy = math.sin(alpha)

            # take my current position
            self.x_origin = self.x_list[-1]
            self.y_origin = self.y_list[-1]

            self.first_move = False


        # flight_length is infinite
        if not self.first_move:
            self.m += 1
            x = (self.vx*self.speed*self.m) + self.x_origin
            y = (self.vy*self.speed*self.m) + self.y_origin  

            if x > self.scenario.max_area:
                x = -self.scenario.max_area + (x-self.scenario.max_area)
                y = y
                self.x_origin = x
                self.y_origin = y
                self.m = 0
            if x < -self.scenario.max_area:
                x = self.scenario.max_area + (x+self.scenario.max_area)
                y = y
                self.x_origin = x
                self.y_origin = y
                self.m = 0
            if y > self.scenario.max_area:
                y = -self.scenario.max_area + (y-self.scenario.max_area)
                x = x
                self.x_origin = x
                self.y_origin = y
                self.m = 0
            if y < -self.scenario.max_area:
                y = self.scenario.max_area + (y+self.scenario.max_area)
                x = x
                self.x_origin = x
                self.y_origin = y
                self.m = 0


            self.x_list.append(x)
            self.y_list.append(y)
           
            # Use only when we don't want to store every position in the list but only the current position. We are now 
            # storing everything in the previous 2 lines of code.
            # self.x_list[-1]=x
            # self.y_list[-1]=y
            

    # def randomDirection(self,c):
    #     self.me_salgo = 0
    #     # initial position m = 1
    #     if self.m == 1:
    #         # select an angle
    #         randNum = np.random.uniform()
    #         alpha = 360 * randNum *(math.pi/180)

    #         # vector based on angle
    #         self.vx = math.cos(alpha)
    #         self.vy = math.sin(alpha)

    #         # take my current position
    #         self.x_origin = self.x_list[-1]
    #         self.y_origin = self.y_list[-1]


    #     # flight_length is infinite
    #     if self.m <= self.flight_length:
    #         x = (self.vx*self.speed*self.m) + self.x_origin
    #         y = (self.vy*self.speed*self.m) + self.y_origin

    #         m = (y - self.y_origin) / (x - self.x_origin)
    #         n = y - (m*x)  
    #         self.m += 1

    #         if x > self.scenario.max_area:
    #             x = self.scenario.max_area
    #             y = m*x + n
    #             self.m = 1
    #             self.me_salgo = self.me_salgo + 1
    #         if x < -self.scenario.max_area:
    #             x = - self.scenario.max_area
    #             y = m*x + n
    #             self.m = 1
    #             self.me_salgo = self.me_salgo + 1
    #         if y > self.scenario.max_area:
    #             y = self.scenario.max_area
    #             x = (y-n)/m
    #             self.m = 1
    #             self.me_salgo = self.me_salgo + 1
    #         if y < -self.scenario.max_area:
    #             y = - self.scenario.max_area
    #             x = (y-n)/m
    #             self.m = 1
    #             self.me_salgo = self.me_salgo + 1


    #         while self.me_salgo > 0:
    #             randNum = np.random.uniform()
    #             alpha = 360 * randNum *(math.pi/180)

    #             self.x_origin = self.x_list[-1]
    #             self.y_origin = self.y_list[-1]

    #             self.vx = math.cos(alpha)
    #             self.vy = math.sin(alpha)
                
    #             x = (self.vx*self.speed*self.m) + self.x_origin
    #             y = (self.vy*self.speed*self.m) + self.y_origin

    #             m = (y - self.y_origin) / (x - self.x_origin)
    #             n = y - (m*x)  
    #             self.m += 1

    #             if x > self.scenario.max_area:
    #                 x = self.scenario.max_area
    #                 y = m*x + n
    #                 self.m = 1
    #             if x < -self.scenario.max_area:
    #                 x = - self.scenario.max_area
    #                 y = m*x + n
    #                 self.m = 1
    #             if y > self.scenario.max_area:
    #                 y = self.scenario.max_area
    #                 x = (y-n)/m
    #                 self.m = 1
    #             if y < -self.scenario.max_area:
    #                 y = - self.scenario.max_area
    #                 x = (y-n)/m
    #                 self.m = 1
                    
    #             if x < self.scenario.max_area and x > -self.scenario.max_area and y < self.scenario.max_area and y > -self.scenario.max_area:
    #                 self.me_salgo = 0
    #                 self.m = 2

            
    #         self.x_list.append(x)
    #         self.y_list.append(y)
           
    #         # Use only when we don't want to store every position in the list but only the current position. We are now 
    #         # storing everything in the previous 2 lines of code.
    #         # self.x_list[-1]=x
    #         # self.y_list[-1]=y

      
    # Method to read from the traces (stored in the scenario) each node's new position
    # This method will make a node move in every new slot to the next point in the list
    def readTraces(self,c):
        if c in self.scenario.tracesDic[self.id]:
            items = self.scenario.tracesDic[self.id][c]
            x = items[0]
            y = items[1]
            # speed = items[2]

            # print("Next point: ", x, y)   
            self.x_list.append(x)
            self.y_list.append(y)

        else:
            self.x_list.append(self.x_list[-1])
            self.y_list.append(self.y_list[-1])

        # self.calculateZones(c)

    def predict(self,nslots):
        for c in range(nslots):
            if c in self.scenario.tracesDic[self.id]:
                items = self.scenario.tracesDic[self.id][c]
                x = items[0]
                y = items[1]
                for z in self.scenario.zois_list:
                    if self.scenario.city =="Paderborn" or self.scenario.city =="Luxembourg" or self.scenario.city =="none":
                        d = np.power(x - z.x,2) + np.power(y - z.y,2)
                    if self.scenario.city !="Paderborn" and self.scenario.city !="Luxembourg" and self.scenario.city != "none":
                        coords_1 = (z.x, z.y)
                        coords_2 = (x, y)
                        d = geopy.distance.distance(coords_1, coords_2).m
                    if d < z.scenario.square_radius_of_replication:
                        self.myFuture[c] = z.id
                    if d < z.scenario.square_radius_of_interest:
                        self.myFuture[c] = z.id
                    if d > z.scenario.square_radius_of_replication:
                        if c not in self.myFuture:
                            self.myFuture[c] = -1 
            else:
                if c == 0:
                    # print(self.id,self.scenario.tracesDic[self.id].keys()[0])
                    first_c = list(self.scenario.tracesDic[self.id].keys())[0]
                    items = self.scenario.tracesDic[self.id][first_c]

                    x = items[0]
                    y = items[1]
                    for z in self.scenario.zois_list:
                        if self.scenario.city =="Paderborn" or self.scenario.city =="Luxembourg" or self.scenario.city =="none":
                            d = np.power(x - z.x,2) + np.power(y - z.y,2)
                        if self.scenario.city !="Paderborn" and self.scenario.city !="Luxembourg" and self.scenario.city != "none":
                            coords_1 = (z.x, z.y)
                            coords_2 = (x, y)
                            d = geopy.distance.distance(coords_1, coords_2).m
                        if d < z.scenario.square_radius_of_replication:
                            self.myFuture[c] = z.id
                        if d < z.scenario.square_radius_of_interest:
                            self.myFuture[c] = z.id
                        if d > z.scenario.square_radius_of_replication:
                            if c not in self.myFuture:
                                self.myFuture[c] = -1 
                else:
                    self.myFuture[c] = self.myFuture[c-1]

    def userContact(self,c):
        # print ("My id is ", self.id, " Am I busy for this slot: ", self.busy)
        my_rep_zones = []
        my_inter_zones = []
        if "replication" in self.zones.values():
            my_rep_zones.append(list(self.zones.keys())[list(self.zones.values()).index("replication")])
        if "interest" in self.zones.values():
            my_inter_zones.append(list(self.zones.keys())[list(self.zones.values()).index("interest")])

        my_rep_zones.extend(my_inter_zones)

        # Include the neighbours found in this slot for contacts statistics
        for user in self.scenario.usr_list:
            if user.id != self.id: #and user.myFuture[c] != -1:
                if self.scenario.city =="Paderborn" or self.scenario.city =="Luxembourg" or self.scenario.city == "none" :
                    pos_user = np.power(user.x_list[-1]-self.x_list[-1],2) + np.power(user.y_list[-1]-self.y_list[-1],2)
                if self.scenario.city !="Paderborn" and self.scenario.city !="Luxembourg" and self.scenario.city != "none" :
                    coords_1 = (user.x_list[-1], user.y_list[-1])
                    coords_2 = (self.x_list[-1], self.y_list[-1])
                    pos_user = geopy.distance.distance(coords_1, coords_2).m
 
                #if self.myFuture[c] != -1:
                if pos_user < self.scenario.square_radius_of_tx:
                    if user.id in self.scenario.list_of_static_nodes or self.id in self.scenario.list_of_static_nodes:
                        self.contacts_per_slot_static[c].append(user.id)
                    if user.id not in self.scenario.list_of_static_nodes and self.id not in self.scenario.list_of_static_nodes:
                        self.contacts_per_slot_dynamic[c].append(user.id)
               

        # Check if the node is not BUSY already for this slot and if the it is in the areas where data exchange is allowed
        if self.busy is False and len(my_rep_zones)>0:
            self.neighbours_list = []
            # Find neighbours in this user's tx range
            for user in self.scenario.usr_list:
                if user.id != self.id:
                    # pos_user = np.power(user.x_list[-1]-self.x_list[-1],2) + np.power(user.y_list[-1]-self.y_list[-1],2)
                    if self.scenario.city =="Paderborn" or self.scenario.city =="Luxembourg" or self.scenario.city =="none":
                        pos_user = np.power(user.x_list[-1]-self.x_list[-1],2) + np.power(user.y_list[-1]-self.y_list[-1],2)
                    if self.scenario.city != "Paderborn" and self.scenario.city !="Luxembourg" and self.scenario.city != "none":
                        coords_1 = (user.x_list[-1], user.y_list[-1])
                        coords_2 = (self.x_list[-1], self.y_list[-1])
                        pos_user = geopy.distance.distance(coords_1, coords_2).m
                      
                    if pos_user < self.scenario.square_radius_of_tx:
                        # Check if the neighbour is in the areas where data exchange is allowed
                        user_rep_zones = []
                        user_inter_zones = []
                        if "replication" in user.zones.values():
                            user_rep_zones.append(list(user.zones.keys())[list(user.zones.values()).index("replication")])
                        if "interest" in user.zones.values():
                            user_inter_zones.append(list(user.zones.keys())[list(user.zones.values()).index("interest")])

                        user_rep_zones.extend(user_inter_zones)
                        p = set(my_rep_zones)&set(user_rep_zones)
                        if len(p) > 0:
                            self.neighbours_list.append(user)
                            # print("This is my neighbour: ", user.id, user.busy)

            # Suffle neighbours list to void connecting always to the same users
            np.random.shuffle(self.neighbours_list)
           
            # Once we have the list of neighbours, first check if there is a previous connection ongoing and the peer is still inside my tx range
            # which is the same as been in the neighbours list since we checked the positions above
            if self.ongoing_conn == True and self.prev_peer in self.neighbours_list:
                # print("I have a prev peer and it is still close. ", self.prev_peer.id)
                self.connection_duration += 1
                self.prev_peer.connection_duration += 1
                # keep exchanging
                self.db_exchange = False
                self.prev_peer.db_exchange = False
                self.exchangeModel(self.prev_peer,c)

            # else exchange data with a probability and within a channel rate per slot
            else:
                # if my prev peer is not in my communication range we don't exchange data anymore
                if self.ongoing_conn == True and self.prev_peer not in self.neighbours_list:
                    # print("I have a prev peer and it is far. ", self.prev_peer.id)
                    if self.connection_duration not in self.scenario.connection_duration_list.keys():
                        self.scenario.connection_duration_list[self.connection_duration] = 1
                    else:
                        self.scenario.connection_duration_list[self.connection_duration] +=1

                    self.connection_duration = 0
                    self.prev_peer.connection_duration = 0
                    # If in previous slot we have exchanged bits from next messages we have to remove them from the used memory because we did't manage to
                    # exchange the whole message so we loose it. Basically --> only reset used_memory because the msg has not been added to the list.
                    reset_used_memory = 0
                    for m in self.model_list:
                        reset_used_memory = reset_used_memory + m.size
                    self.used_memory = reset_used_memory
                    reset_used_memory = 0
                    for m in self.prev_peer.model_list:
                        reset_used_memory = reset_used_memory + m.size
                    self.prev_peer.used_memory = reset_used_memory

                    # reset all parameters to start clean with a new peer
                    self.exchange_list = []
                    self.prev_peer.exchange_list = []
                    self.exchange_size = 0  
                    self.prev_peer.exchange_size = 0
                    self.db_exchange = False
                    self.prev_peer.db_exchange = False
                    self.ongoing_conn = False
                    self.prev_peer.ongoing_conn = False
                    # Set back the used mbs for next data exchange for next slot
                    self.scenario.used_mbs = 0
                
                # Continue looking for neighbours   
                # print("Neighbour list: ", len(self.neighbours_list))
                # In case we want to connect with more than one neighbour we need to run a loop. Now we only select one neighbour from the list.
                neighbour = None
                for neig in self.neighbours_list:
                        if not neig.busy and neig.ongoing_conn == False:
                            neighbour = neig
                            # print("I found a peer not busy and without ongoing connection. ", neighbour.id)
                            break
                if neighbour != None:
                    self.existing1 = True
                    self.existing2 = True
                    neighbour.existing1 = True
                    neighbour.existing2 = True

                    self.scenario.attempts +=1
                    # print("Attempts--- ", self.scenario.attempts)
                    self.connection_duration += 1
                    neighbour.connection_duration +=  1
                    self.exchange_size = 0
                    neighbour.exchange_size = 0
                    self.exchange_list = []
                    neighbour.exchange_list = []
                    self.exchange_counter = 0
                    neighbour.exchange_counter = 0
                    self.counter_list = []
                    neighbour.counter_list = []
                    self.db_exchange = False
                    neighbour.db_exchange = False
                    self.scenario.used_mbs = 0
                    # First, check the messages missing in the peers devices and add them to the exchange list of messages of every peer
                    for m1 in self.model_list:
                        if m1 not in self.exchange_list:
                        # if m not in neighbour.pending_model_list and m not in neighbour.model_list and m not in self.exchange_list:
                            for pickCont1 in m1.contributions.keys():
                                if len(neighbour.pending_model_list) > 0:
                                    for nm1 in neighbour.pending_model_list: 
                                        if pickCont1 not in nm1.contributions.keys():
                                            self.existing1 = False
                                            if self.id == 33:
                                                print(pickCont1)
                                                print("las otras contributions le falta a pendings--->", nm1.contributions.keys())
                                            break
                                if len(neighbour.model_list) > 0:
                                    for nm2 in neighbour.model_list:
                                        if pickCont1 not in nm2.contributions.keys():
                                            self.existing2 = False
                                            if self.id == 33:
                                                print(pickCont1)
                                                print("las otras contributions le falta al modelo--->", nm2.contributions.keys())
                                            break

                                if len(neighbour.pending_model_list) == 0:
                                    self.existing1 = False
                                    if self.id == 33:
                                        print("no tiene pendings el neighbour!")
                                if len(neighbour.model_list) == 0:
                                    self.existing2 = False
                                    if self.id == 33:
                                        print("no tiene models el neighbour!")

                            

                            if not self.existing1 and not self.existing2:
                                self.existing1 = True
                                self.existing2 = True
                                if m1 not in self.exchange_list:
                                    self.exchange_list.append(m1.copy())
                                    self.exchange_size = self.exchange_size + m1.size
                                    if len(self.counter_list) == 0:
                                        self.counter_list.append(m1.size)
                                    else:
                                        self.counter_list.append(self.counter_list[-1]+m1.size)

                                    if self.id == 33:
                                        print("entro en meter el modelo en exchange list")
                                        print("self contributions",m1.contributions.keys())
                                        print("self--->",self.existing1, self.existing2)

                    np.random.shuffle(self.exchange_list)
                    
                    for m2 in neighbour.model_list:
                        if m2 not in neighbour.exchange_list:
                        # if m not in self.pending_model_list and m not in self.model_list and m not in neighbour.exchange_list: 
                            for pickCont2 in m2.contributions.keys():

                                if len(self.pending_model_list) > 0:
                                    for nm3 in self.pending_model_list:    
                                        if pickCont2 not in nm3.contributions.keys():
                                            neighbour.existing1 = False
                                            if self.id == 33:
                                                print(pickCont2)
                                                print("las otras contributions le falta a pendings--->", nm3.contributions.keys())
                                            break

                                if len(self.model_list) > 0:
                                    for nm4 in self.model_list:  
                                        if pickCont2 not in nm4.contributions.keys():
                                            neighbour.existing2 = False
                                            if self.id == 33:
                                                print(pickCont2)
                                                print("las otras contributions le falta al modelo--->", nm4.contributions.keys())
                                            break

                                if len(self.pending_model_list) == 0:
                                    neighbour.existing1 = False
                                    if self.id == 33:
                                        print("no tengo pendings!")
                                if len(self.model_list) == 0:
                                    neighbour.existing2 = False
                                    if self.id == 33:
                                        print("no tengo models!")

                            if not neighbour.existing1 and not neighbour.existing2:
                                neighbour.existing1 = True
                                neighbour.existing2 = True
                                if m2 not in neighbour.exchange_list:
                                    neighbour.exchange_list.append(m2.copy())
                                    neighbour.exchange_size = neighbour.exchange_size + m2.size
                                    if len(neighbour.counter_list) == 0:
                                        neighbour.counter_list.append(m2.size)
                                    else:
                                        neighbour.counter_list.append(neighbour.counter_list[-1]+m2.size)

                                    if self.id == 33:
                                        print("Entro en meter el modelo en exchange list")
                                        print("neighbour contributions",m2.contributions.keys())
                                        print("neighbour--->",neighbour.existing1, neighbour.existing2)

                    # After choosing the messages that are missing in the peer, we need to shuffle the list
                    np.random.shuffle(neighbour.exchange_list)

                    # Second, exchange the data with peer!!
                    # Count in advance if the connection is going to be useful or not, it means if they have something to exchange.
                    # In case we have nothing to exchange we use the last slot for the checking
                    if self.exchange_size == 0 and neighbour.exchange_size == 0:
                        self.scenario.count_non_useful +=1
                    else:
                        self.scenario.count_useful +=1

                    self.exchangeModel(neighbour,c)
                        
                    
    # Method to check which DB is smaller and start exchanging it. 
    # At this point We have the messages to be exchange (exchange_list) and the total list sizes (exchange_size).

    def exchangeModel(self,neighbour,c):
        self.busy = True
        neighbour.busy = True
        self.ongoing_conn = True
        neighbour.ongoing_conn = True

        if self.exchange_size == 0 and neighbour.exchange_size == 0:
            self.db_exchange = True
            neighbour.db_exchange= True

        if self.exchange_size <= neighbour.exchange_size and self.db_exchange is False:
            howMany = 0
            howMany1 = 0
            howMany2 = 0

            # print("My db is smaller than neighbours ", self.exchange_size)
            if self.exchange_size == 0:
                self.db_exchange = True
            else: 
                #########################################################################################################
                if (self.exchange_counter < self.exchange_size):
                    howMany = self.exchange_size - self.exchange_counter
                    howMany1 = self.exchange_size - self.exchange_counter
                    howMany2 = self.exchange_size - self.exchange_counter

                    # Check if the amount of bits to transfer (self.exchange_size) fits in the available channel rate
                    if (howMany > ((self.scenario.mbs/2) - self.scenario.used_mbs)):
                        howMany1 = (self.scenario.mbs/2) - self.scenario.used_mbs
                        # print("1 Mensaje mas grande que mbs, para que quepa: ",howMany1)
                    if (neighbour.used_memory + howMany > neighbour.total_memory):
                        howMany2 = neighbour.total_memory - neighbour.used_memory
                        # print("1 Mensaje mas grande que memoria, para que quepa: ",howMany2)

                    howMany = min(howMany1,howMany2)
                    
                    self.exchange_counter += howMany
                    self.scenario.used_mbs += howMany
                    neighbour.used_memory += howMany

                    self.db_exchange = True 
                    # print("I send X bits: ", self.exchange_counter)
                    # print("used memory: ", neighbour.used_memory) 
                    # print(self.scenario.mbs, self.scenario.used_mbs)

            self.scenario.used_mbs_per_slot.append(self.scenario.used_mbs)   
            #########################################################################################################
            # print("Now we continue with Neigbours db", neighbour.exchange_size)
            if neighbour.exchange_size == 0:
                neighbour.db_exchange = True
            else:
                if (neighbour.exchange_counter < neighbour.exchange_size):
                    howMany = neighbour.exchange_size - neighbour.exchange_counter
                    howMany1 = neighbour.exchange_size - neighbour.exchange_counter
                    howMany2 = neighbour.exchange_size - neighbour.exchange_counter

                    # Check if the amount of bits to transfer (neighbour.exchange_size) fits in the available channel rate
                    if(howMany > (self.scenario.mbs - self.scenario.used_mbs)):
                        howMany1 = self.scenario.mbs - self.scenario.used_mbs
                        # print("2 Mensaje mas grande que mbs, para que quepa: ",howMany1)
                    if (self.used_memory + howMany > self.total_memory):
                        howMany2 = self.total_memory - self.used_memory
                        # print("2 Mensaje mas grande que memoria, para que quepa: ",howMany2)
        

                    howMany = min(howMany1,howMany2)
                
                    neighbour.exchange_counter += howMany
                    neighbour.scenario.used_mbs += howMany
                    self.used_memory += howMany
                    
                    neighbour.db_exchange = True  
                    # print("Neighbour sends me X bits: ", neighbour.exchange_counter)
                    # print("used memory: ", self.used_memory)
                    # print(self.scenario.mbs, self.scenario.used_mbs)
            neighbour.scenario.used_mbs_per_slot.append(howMany)
        #########################################################################################################
        if neighbour.exchange_size < self.exchange_size and neighbour.db_exchange is False:
            howMany = 0
            howMany1 = 0
            howMany2 = 0
            # print("Neighbour db is smaller than mine", neighbour.exchange_size)
            if neighbour.exchange_size == 0:
                neighbour.db_exchange = True
            else:
                if (neighbour.exchange_counter < neighbour.exchange_size):
                    howMany = neighbour.exchange_size - neighbour.exchange_counter
                    howMany1 = neighbour.exchange_size - neighbour.exchange_counter
                    howMany2 = neighbour.exchange_size - neighbour.exchange_counter

                    if(howMany > ((self.scenario.mbs/2) - self.scenario.used_mbs)):
                        howMany1 = (self.scenario.mbs/2) - self.scenario.used_mbs
                        # print("3 Mensaje mas grande que mbs, para que quepa: ",howMany1)
                    if(self.used_memory + howMany > self.total_memory):
                        howMany2 = self.total_memory - self.used_memory
                        # print("3 Mensaje mas grande que memory, para que quepa: ",howMany2)
                
                    
                    howMany = min(howMany1,howMany2)
                        
                    neighbour.exchange_counter += howMany
                    neighbour.scenario.used_mbs += howMany
                    self.used_memory += howMany

                    neighbour.db_exchange = True  
                    # print("Neighbour sends me one bit: ", neighbour.exchange_counter)
                    # print("used memory: ", self.used_memory)
                    # print(self.scenario.mbs, self.scenario.used_mbs)
            neighbour.scenario.used_mbs_per_slot.append(self.scenario.used_mbs)
            #########################################################################################################
            # print("Now we continue with my db", self.exchange_size)
            if self.exchange_size == 0:
                self.db_exchange = True
            else:
                if (self.exchange_counter < self.exchange_size):
                    howMany = self.exchange_size - self.exchange_counter
                    howMany1 = self.exchange_size - self.exchange_counter
                    howMany2 = self.exchange_size - self.exchange_counter
                    
                    if(howMany > (self.scenario.mbs - self.scenario.used_mbs)):
                        howMany1 = self.scenario.mbs - self.scenario.used_mbs
                        # print("4 Mensaje mas grande que mbs, para que quepa: ",howMany1)
                    if(neighbour.used_memory + self.exchange_size > neighbour.total_memory):
                        howMany2 = neighbour.total_memory - neighbour.used_memory
                        # print("4 Mensaje mas grande que memory, para que quepa: ",howMany2)
                    
                    howMany = min(howMany1,howMany2)
                    
                    self.exchange_counter += howMany
                    neighbour.used_memory += howMany
                    self.scenario.used_mbs += howMany

                    self.db_exchange = True  
                    # print("I send one bit: ", self.exchange_counter)
                    # print("used memory: ", neighbour.used_memory)
                    # print(self.scenario.mbs, self.scenario.used_mbs)
            self.scenario.used_mbs_per_slot.append(howMany)
            #########################################################################################################

        # Now we exchange the db based on the already exchanged bytes of messages
        if len(self.exchange_list) > 0:
            for i in range(0,len(self.counter_list)): 
                if (self.counter_list[i] <= self.exchange_counter):
                    if neighbour.id == 33:
                        print(self.id,"Adding model to neighbour DB: ", len(neighbour.pending_model_list))
                        
                    neighbour.pending_model_list.append(self.exchange_list[i].copy())
                    if neighbour.id == 33:
                        print(self.id,"Adding model to neighbour DB 2: ", len(neighbour.pending_model_list))
                        print(self.exchange_list[i].contributions.keys())
                        
                    if len(self.exchange_list[i].contributions) > 0:
                        obs = OrderedDict()
                        for k in range(self.scenario.num_landmarks):
                            obs[k] = 0
                        for o,co in self.exchange_list[i].contributions.items():
                            if o[0] > obs[o[1]]:
                                obs[o[1]] = o[0]
                        for k in range(self.scenario.num_landmarks): 
                            fresh = c - obs[k]
                            neighbour.freshness[k].append(fresh)

                if(self.counter_list[i] == self.exchange_counter):
                    break
                    
        if len(neighbour.exchange_list) > 0:
            for j in range(0,len(neighbour.counter_list)):
                if (neighbour.counter_list[j] <= neighbour.exchange_counter): 
                    if self.id == 33:
                        print(self.id,"Adding model to my DB: ", len(self.pending_model_list))
                    self.pending_model_list.append(neighbour.exchange_list[j].copy())

                    if self.id == 33:
                        print(self.id,"Adding model to my DB: ", len(self.pending_model_list))
                        print(neighbour.exchange_list[j].contributions.keys())
                   
                    if len(neighbour.exchange_list[j].contributions) > 0:
                        obs = OrderedDict()
                        for k in range(self.scenario.num_landmarks):
                            obs[k] = 0
                        for o,co in neighbour.exchange_list[j].contributions.items():
                            if o[0] > obs[o[1]]:
                                obs[o[1]] = o[0]
                        for k in range(self.scenario.num_landmarks): 
                            fresh = c - obs[k]
                            self.freshness[k].append(fresh)

                if(neighbour.counter_list[j] == neighbour.exchange_counter):
                    break

        # After exchanging both peers part of the db, set back the booleans for next slot
        self.db_exchange = False
        neighbour.db_exchange = False
        # Set back the used mbs for next data exchange for next slot
        self.scenario.used_mbs = 0

        # If any of the peers DB has not been totally exchanged we have to store the peer device to keep the connection for next slot
        # print("COMPROBAR---------> ",self.exchange_counter, self.exchange_size, neighbour.exchange_counter, neighbour.exchange_size,len(self.model_list),len(neighbour.model_list))
        if self.exchange_counter < self.exchange_size or neighbour.exchange_counter < neighbour.exchange_size:
            self.prev_peer = neighbour
            # print(" PASSING NEIGHBOUR TO PREV DB", neighbour.id, self.prev_peer.id, neighbour == self.prev_peer)
            self.ongoing_conn = True
            self.prev_peer.ongoing_conn = True
            self.prev_peer.prev_peer = self
            
        # If everything has been exchanged, reset parameters
        if (self.exchange_counter == self.exchange_size and neighbour.exchange_counter == neighbour.exchange_size) or (self.total_memory == self.used_memory and neighbour.total_memory == neighbour.used_memory):
            # print("EVERYTHING HAS BEEN EXCHANGED WITH: ", self.exchange_counter,self.exchange_size)
            # print("ENTRO AQUI", self.exchange_counter, self.exchange_size,neighbour.exchange_counter, neighbour.exchange_size, self.used_memory, self.used_memory)
            if self.connection_duration not in self.scenario.connection_duration_list.keys():
                self.scenario.connection_duration_list[self.connection_duration] = 1
            else:
                self.scenario.connection_duration_list[self.connection_duration] +=1
            # print("CONNEC DURATION normal--> ", self.connection_duration)

    
            self.connection_duration = 0
            neighbour.connection_duration = 0
            self.ongoing_conn = False
            neighbour.ongoing_conn = False
            self.exchange_list = []
            neighbour.exchange_list = []
            self.db_exchange = False
            neighbour.db_exchange = False
            self.counter_list = []
            neighbour.counter_list = []
            self.exchange_counter = 0
            neighbour.exchange_counter = 0
            self.exchange_size = 0
            neighbour.exchange_size = 0
            self.scenario.used_mbs = 0
            self.hand_shake_counter = 0
            neighbour.hand_shake_counter = 0
            # If they don't have anything to exchange from the beginning they will not be set as busy for this slot.
            # They will remain busy just in case that during this slot they finished exchanging their DB.
            if neighbour.exchange_size == 0 and self.exchange_size == 0:
                self.busy = False
                neighbour.busy = False


    def computeTask(self,c):

        if self.out:
            if self.id == 33:
                print(self.id,"stoy out")
            self.computing_counter = 0
            self.merging_counter = 0
            self.list_to_merge = []
           
        if not self.out:
            if self.id == 33:
                print(self.id,"estoy in",self.merging_counter,self.computing_counter,len(self.pending_model_list))
            # no he empezado a ejecutar ninguna tarea y tengo tareas pendientes
            if self.computing_counter == 0 and len(self.pending_model_list) > 0 and self.merging_counter == 0:

                #it is the first time I have to do the merge
                if self.id == 33:
                    print(self.id,"entro en merge por primera vez",len(self.pending_model_list))           
                self.scenario.merging_mean_rate.append(c)

                # primero meto todos los pendings con el mismo id en la lista para merge
                for pend_model in self.pending_model_list:
                    if pend_model.id == self.pending_model_list[0].id:
                        self.list_to_merge.append(pend_model)

            
                # borro todos los pendings porque ya los he metido en merge
                # no hacer cuando hay mas de un modelo distinto, con distinto id
                # self.pending_model_list = []
                
                # segundo hago un merge de los pendings
                for pm in self.list_to_merge:
                    if pm != self.list_to_merge[0]:
                        self.list_to_merge[0].contributions.update(pm.contributions)

                # borro todos los merge menos el primero
                merged_model = self.list_to_merge[0]
                self.list_to_merge = []
                self.list_to_merge.append(merged_model)


                if len(self.observations) > 0:
                    if self.id == 33:
                        print(self.id,"tengo observations")
                    # if I have an observation that is not in my stored model neither in the merged model, then I have to train the model
                    if len(self.model_list)> 0:
                        for model in self.model_list:
                            if model.id == self.list_to_merge[0].id:
                                for lm, slots in self.observations.items():
                                    for slot in slots:
                                        observation = tuple((slot,lm,self.id))
                                        if observation not in model.contributions.keys() and observation not in self.list_to_merge[0].contributions.keys(): 
                                            self.observation_to_train = observation
                                            self.merging_counter = self.merging_counter + 1
                                            if self.id == 33:
                                                print(self.id,"encuentro observation que no esta en ninguna lista",self.observation_to_train)
                                            break

                    
                    if len(self.model_list) == 0:
                        for lm, slots in self.observations.items():
                            for slot in slots:
                                observation = tuple((slot,lm,self.id))
                                if observation not in self.list_to_merge[0].contributions.keys(): 
                                    self.observation_to_train = observation
                                    self.merging_counter = self.merging_counter + 1
                                    if self.id == 33:
                                        print(self.id,"encuentro observation que no esta en merge (no tengo model)",self.observation_to_train)
                                    if self.observation_to_train == tuple((5003, 0, 33)):
                                        print("obser",self.observation_to_train,"-------")
                                    
                                    break
                    

                if len(self.observations) == 0:
                    if self.id == 33:
                        print(self.id,"no tengo observations")
                    # if I have no observation I include the merged pendings in my model list directly

                    self.merging_counter = self.merging_counter + 1
                    if self.id == 33:
                        print(self.id,"no tengo observations y no se si modelos",self.merging_counter)

                    if len(self.model_list) > 0:
                        if self.model_list[0].id == self.list_to_merge[0].id:
                            if self.id == 33:
                                print("UPDATESSSS--->", len(self.list_to_merge[0].contributions),len(self.model_list[0].contributions))
                            self.list_to_merge[0].contributions.update(self.model_list[0].contributions)
                            if self.id == 33:
                                print("UPDATESSSS--->", len(self.list_to_merge[0].contributions))
                            self.model_list = []
                            if self.id == 33:
                                print(self.id,"SI modelo",self.merging_counter)



            if self.computing_counter == 0 and len(self.pending_model_list) == 0 and self.merging_counter == 0:
                if self.id == 33:
                    print(self.id, "entro sin pendings") 

                if len(self.observations) > 0:
                    if self.id == 33:
                        print(self.id,"tengo observations sin pendings",len(self.model_list))
                    # if I have an observation that is not in my stored model then I have to train the model
                    if len(self.model_list)> 0:
                        for model in self.model_list:
                            for lm, slots in self.observations.items():
                                for slot in slots:
                                    observation = tuple((slot,lm,self.id))
                                    
                                    if self.id == 33:
                                        print(observation)
                                    if observation not in model.contributions.keys(): 
                                        self.observation_to_train = observation
                                        self.merging_counter = self.scenario.merging_time
                                        # if self.id == 33:
                                        #     print(self.id,"la observation no existia",self.merging_counter)
                                        break

                                    # if observation in model.contributions.keys(): 
                                    #     if self.id == 33:
                                    #         print(self.id,"la observation ya existe",self.merging_counter)


                    # if I have an observation but I have nothing to combine with, then let's start training the first model
                    if len(self.model_list) == 0:
                        for lm, slots in self.observations.items():
                            for slot in slots:
                                observation = tuple((slot,lm,self.id))
                                self.observation_to_train = observation
                                self.merging_counter = self.scenario.merging_time
                                if self.id == 33:
                                    print(self.id,"entro en sin pendings pero con observations y SIN modelo",self.merging_counter)
                                break

                if len(self.observations) == 0:
                    if self.id == 33:
                        print(self.id,"tampoco tengo observations")

    

            # if I just need to increase the merging counter, go here
            if self.merging_counter > 0 and self.merging_counter < self.scenario.merging_time:
                self.merging_counter = self.merging_counter + 1
                if self.id == 33:
                    print(self.id,"entro en incrementar merging counter ", self.merging_counter)
                        

            if self.merging_counter == self.scenario.merging_time and self.computing_counter == 0: 
                if self.id == 33:
                    print("fin del merging")
                self.merging_counter = 0

                if len(self.list_to_merge) > 0:
                    if self.id == 33:
                        print("tengo merged")
                    if len(self.model_list)> 0:
                        if self.id == 33:
                            print("tengo model")
                        if self.model_list[0].id == self.list_to_merge[0].id:
                            if self.id == 33:
                                print("UPDATESSSS--->", len(self.list_to_merge[0].contributions),len(self.model_list[0].contributions))
                            self.list_to_merge[0].contributions.update(self.model_list[0].contributions)
                            if self.id == 33:
                                print("UPDATESSSS--->", len(self.list_to_merge[0].contributions))
                            self.model_list = []
                    self.model_list.append(self.list_to_merge[0].copy())
                    if self.id == 33:
                        print(self.id, "meto el merged en el modelo")
                    self.list_to_merge = []
                    self.pending_model_list = []

                if len(self.list_to_merge) == 0:
                    if len(self.model_list) == 0:
                        self.model_list.append(self.scenario.zois_list[0].model_list[0].copy())

                        
                if self.observation_to_train:
                    self.computing_counter = self.computing_counter + 1

                if self.observation_to_train == None:
                    self.computing_counter = self.scenario.computing_time


            if self.computing_counter > 0 and self.computing_counter < self.scenario.computing_time:
                self.computing_counter += 1
                if self.id == 33:
                    print(self.id,"entro en computing + 1", self.computing_counter)

            if self.computing_counter == self.scenario.computing_time:
                self.computing_counter = 0
                if self.observation_to_train:
                    self.model_list[0].contributions[self.observation_to_train] = c
                    self.observation_to_train = None
                if self.id == 33:
                    print("termino el computing y anado la obs---- o no habia obs",len(self.model_list))
                    print(self.model_list[0].contributions)