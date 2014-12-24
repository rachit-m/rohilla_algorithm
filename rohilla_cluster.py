import csv
import psycopg2
import pdb


def cluster_stability(thelist, id_score_map):
    sum = 0
    for k in thelist:
        if k in id_score_map.keys():
            sum = sum + id_score_map[k]
    return sum


def right_clusters():
    with open('rohilla_scores.csv', 'rb') as csvfile:
        spamreader = csv.reader(csvfile, delimiter=',', quotechar='|')
        cluster_sets = []
        right_cluster = dict()
        id_score_map = dict()
        id_group_map = dict()
        for row in spamreader:
            id_score_map[int(row[0])] = float(row[1])
        con = psycopg2.connect(
            "dbname='housing_analytics' user='dsl_readonly' password='dsl' host=127.0.0.1 port = 5436")
        cur = con.cursor()
        cur.execute(
            ' set search_path to public,postgis; select lo1.id,array_agg(lo2.id) from (select  id,boundary_polygon from localities where region_id = 2) lo1 cross join (select id, boundary_polygon from localities where region_id = 2) lo2  where  st_intersects(lo1.boundary_polygon, lo2.boundary_polygon) group by lo1.id;')
        data = cur.fetchall()
        for each in data:
            id_group_map[each[0]] = each[1]  # data in form of L1 G1, L2 G2 ..
            right_cluster[each[0]] = dict()
        for each in data:
                # Cluster Stability Problem
            for ele in each[1]:   # for each element in G1
                if each[0] in id_score_map.keys():
                    right_cluster[each[0]][ele] = cluster_stability(
                        id_group_map[ele], id_score_map) - id_score_map[each[0]]
            if len(right_cluster[each[0]].values()) > 0:
                maxuse = max([abs(round(sum_score) - sum_score)  # taking the max of G2, G3
                              for sum_score in right_cluster[each[0]].values()])
                for ele in each[1]:
                    if each[0] in id_score_map.keys():
                        if maxuse == abs(round(right_cluster[each[0]][ele]) - right_cluster[each[0]][ele]):
                            if id_group_map[ele] not in cluster_sets:
                                # Removing duplicate cluster sets
                                cluster_sets.append(id_group_map[ele])
                        else:
                            # removing G1 from other groups
                            id_group_map[ele].remove(each[0])
    for ele in cluster_sets:
        print ele, cluster_stability(ele, id_score_map)
right_clusters()
