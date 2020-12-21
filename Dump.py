from collections import OrderedDict
import json
import numpy as np


###################### DATA DUMP  ################################################
class Dump:
    'Common base class for all dumps'

    def __init__(self,scenario,uid):
        self.id = 1
        self.scenario= scenario
        self.uid = uid 

    ####### last user's position
    def userLastPosition(self,list_of_static_nodes):
        x = []
        y = []
        z=[]
        l = []
        ids = []
        zois = []
        for i in self.scenario.usr_list:
            x.append(i.x_list[-1])
            y.append(i.y_list[-1])
            z.append(len(i.model_list))
            l.append(i.zones.values())

            # print("User id: ", self.scenario.usr_list[i].id, "position x: ", self.scenario.usr_list[i].x_list[-1] , 
            # "position y: ", self.scenario.usr_list[i].y_list[-1], "zones: ",self.scenario.usr_list[i].zones.values())


        file = open(str(self.uid) +'/userLastPosition.txt', "w")
        file.write(json.dumps(x))
        file.write(json.dumps("&"))
        file.write(json.dumps(y))
        file.write(json.dumps("&"))
        file.write(json.dumps(z))
        file.write(json.dumps("&"))
        file.write(json.dumps(l))
        for h in self.scenario.zois_list:
            file.write(json.dumps("&"))
            file.write(json.dumps(h.x))
            file.write(json.dumps("&"))
            file.write(json.dumps(h.y))
            file.write(json.dumps("&"))
            file.write(json.dumps(self.scenario.radius_of_interest))
            file.write(json.dumps("&"))
            file.write(json.dumps(self.scenario.radius_of_replication))
            file.write(json.dumps("&"))
            file.write(json.dumps(self.scenario.radius_of_replication))

        for i in self.scenario.usr_list:
            ids.append(i.id)

        for i in self.scenario.zois_list:
            zois.append(i.id)
        
        file.write(json.dumps("&"))
        file.write(json.dumps(list(list_of_static_nodes)))
        file.write(json.dumps("&"))
        file.write(json.dumps(ids))
        file.write(json.dumps("&"))
        file.write(json.dumps(zois))
        file.close()

    ####### Lists for statistics 

    def statisticsList(self,slots, zoi_users, zoi, rep_users, rep, per_users, per,attempts):
         np.savetxt(str(self.uid)+'/dump.txt', np.column_stack((slots, zoi_users, zoi, rep_users, rep, per_users, per,attempts)),
         fmt="%i %i %i %i %i %i %i %i")

    ####### Connection duration list

    def connectionDurationAndMore(self,contacts_per_slot_per_user_dynamic, contacts_per_slot_per_user_static,exiting_nodes):
        with open(str(self.uid)+'/contacts-per-slot-per-user-dynamic.json', 'w') as fp:
            json.dump(contacts_per_slot_per_user_dynamic, fp)

        with open(str(self.uid)+'/contacts-per-slot-per-user-static.json', 'w') as fp1:
            json.dump(contacts_per_slot_per_user_static, fp1)

        with open(str(self.uid)+'/connection-duration-list.json', 'w') as fp2:
            json.dump(self.scenario.connection_duration_list, fp2)

        with open(str(self.uid)+'/exiting-nodes.json', 'w') as fp3:
            json.dump(self.scenario.exiting_nodes, fp3)


     ####### Availability per model per slot

    def availabilityPerModel(self,a_per_model):
        with open(str(self.uid)+'/availability-per-model.json', 'w') as fp:
            json.dump(a_per_model, fp)

    ####### Availability per observation per slot

    def availabilityPerObservation(self,a_per_obs):
        with open(str(self.uid)+'/availability-per-observation.json', 'w') as fp:
            json.dump(a_per_obs, fp)

    ####### Availability final point per simulation

    def availabilityPerSimulation(self,printa):
        f = open(str(self.uid)+'/availability_points.txt',"w")
        f.write(str(printa))
        f.close()

    ####### list of availabilities per slot per simulation

    def listOfAveragesPerSlot(self,availabilities_list_per_slot):
        outfile = open(str(self.uid)+'/availability_per_slot_per_sim.txt', 'w')
        for result in availabilities_list_per_slot:
            outfile.writelines(str(result))
            outfile.write('\n')
        outfile.close()

    ########### number of connections that started but didn't finish. With the same number of slots as hand shake + 1 slot to check 
    # that they don't have anything to exchange
    def con0exchange(self):
        f = open(str(self.uid)+'/counters.txt',"w")
        f.write(str(self.scenario.count_0_exchange_conn)+"\n")
        f.write(str(self.scenario.count_non_useful)+"\n")
        f.write(str(self.scenario.count_useful)+"\n")
        f.close()
        

    ####### Number of users in the ZOI per slot

    def nodesZoiPerSlot(self,nodes_in_zoi):
        with open(str(self.uid)+'/nodes-in-zoi.json', 'w') as fp:
            json.dump(nodes_in_zoi, fp)


    ####### Path followed by every node

    def nodesPath(self):
        outfile = open(str(self.uid)+'/nodes-path.txt', 'w')
        for n in self.scenario.usr_list:
            outfile.writelines(str(n.id)+"\n")
            outfile.writelines(str(n.x_list)+"\n")
            outfile.writelines(str(n.y_list)+"\n")
        outfile.close()


    ####### Contributions in models

    def modelContributions(self,models_contributions):
        with open(str(self.uid)+'/model-contributions.json', 'w') as fp:
            json.dump(models_contributions, fp)

    ####### Freshness in models

    def modelFreshness(self,models_freshness):
        with open(str(self.uid)+'/model-freshness.json', 'w') as fp:
            json.dump(models_freshness, fp)

     ####### Nodes future zois

    def nodesFuture(self,nodes_future):
        with open(str(self.uid)+'/nodes-future.json', 'w') as fp:
            json.dump(nodes_future, fp)

    ####### merging mean rate

    def mergingMeanRate(self,merging_mean_rate):
        with open(str(self.uid)+'/merging-mean-rate.json', 'w') as fp:
            json.dump(merging_mean_rate, fp)

    ####### observations mean rate

    def observationsMeanRate(self,observations_mean_rate):
        with open(str(self.uid)+'/observations-mean-rate.json', 'w') as fp:
            json.dump(observations_mean_rate, fp)

        