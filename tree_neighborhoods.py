# import packages/modules
import pandas as pd
import mysql.connector
import config
import mysql_functions as my_funcs
from sodapy import Socrata
from geopy.distance import geodesic
import random
import itertools
import numpy as np
#from sklearn.neighbors import KDTree
from sklearn.neighbors import NearestNeighbors
pd.set_option('display.max_columns', None)

def get_neighbors(tree_loc, spc, df):
    #gets geodesic distance between singular tree and a group of trees. Then filters down to trees within 1000 feet. Note that each tree is a neighbor of itself
    df['distance'] = df.apply(lambda row: geodesic(row['loc_tuple'], tree_loc).feet,  axis = 1)
    neighbors_df = (df[df['distance'] < 500])
    #print('here 2')
    neighbor_tuple = (neighbors_df.shape[0], neighbors_df['spc_latin'].nunique(), neighbors_df[neighbors_df['spc_latin'] == spc].shape[0])
    #neighbor tuple consists of (total_neighbors, distinct species neighbors, same species neighbors)
    return neighbor_tuple

def apply_neighbors(df):
    df['loc_tuple'] = df.apply(lambda row: (row['latitude'], row['longitude']), axis = 1)
    print('here 1')
    print(df.shape)
    df[['total_neighbors', 'distinct_spc_neighbors', 'same_spc_neighbors']] = df.apply(lambda row: pd.Series(get_neighbors(row['loc_tuple'], row['spc_latin'], df[df['zipcode'] == row['zipcode']])), axis = 1)
    #df[['total_neighbors', 'distinct_spc_neighbors', 'same_spc_neighbors']] = df.apply(lambda row: pd.Series(get_neighbors(row['loc_tuple'], row['spc_latin'], df[df['zip_new'] == row['zip_new']])), axis = 1)
    df_insert = df[['total_neighbors', 'distinct_spc_neighbors', 'same_spc_neighbors', 'tree_id']]
    return df

# def draw_sample(year, borough = 'any', zip_code = 'any'):
#     table = 'trees_' + str(year)
#     if zip_code == 'any':
#         zip_list = my_funcs.get_zip_list(table, borough)
#         #get list of zips by borough
#         zip_code = random.sample(zip_list, 1)[0]
#         #draw a random zip from that list
#     else:
#         zip_code = str(zip_code)
#     zip_df = pd.DataFrame(my_funcs.get_data_by_zip(str(zip_code), table), columns = ['tree_id', 'health', 'spc_latin', 'zipcode', 'boroname', 'latitude', 'longitude'])
#     zip_df['loc_tuple'] = zip_df.apply(lambda row: (row['latitude'], row['longitude']), axis = 1)
#     zip_df['total_neighbors'] = 0
#     zip_df['same_spc_neighbors'] = 0
#     zip_df['neighbor_tree_set'] = pd.Series([])
#     print(len(list(zip_df['tree_id'])))
#     zip_df.set_index('tree_id')
#     all_tree_pairs = itertools.combinations(list(zip_df['tree_id']), 2)
#     for pair in all_tree_pairs:
#         tree_1 = zip_df[zip_df['tree_id'] == pair[0]]
#         tree_2 = zip_df[zip_df['tree_id'] == pair[1]]
#         both_trees = pd.Series([tree_1['spc_latin'].iloc[0], tree_2['spc_latin'].iloc[0]])
#         if geodesic(tree_1['loc_tuple'], tree_2['loc_tuple']).feet <= 500:
#             zip_df.loc[zip_df['tree_id'].isin(list(pair)), 'total_neighbors'] += 1
#             zip_df.loc[zip_df['tree_id'].isin(list(pair)), 'neighbor_tree_set'].append(both_trees)
#             #print(both_trees)
#             #print(tree_1['total_neighbors'])
#             #print(tree_1['spc_latin'].iloc[0])
#             #print(tree_2['spc_latin'].iloc[0])
#
#             if tree_1['spc_latin'].iloc[0] == tree_2['spc_latin'].iloc[0]:
#                 zip_df.loc[zip_df['tree_id'].isin(list(pair)), 'same_spc_neighbors'] += 1
#
#     print(zip_df)
#
#     return

#draw_sample('2015')


def get_tree_nerighborhood(df, neigh, tree_loc):

    print(tree_loc.columns)

    row_id = list(tree_loc.index)
    #print(row_id)
    full_tree = df.loc[row_id]
    #print(full_tree.columns)
    #loc_neigh = pd.DataFrame(neigh.radius_neighbors(tree_loc))
    #loc_neigh = pd.DataFrame(neigh.radius_neighbors(tree_loc), columns = ['tree_id', 'latitude', 'longitude'] )
    #loc_neigh = df.set_index('tree_id')
    #print(loc_neigh.columns)
    #loc_neigh['loc_tuple'] = loc_neigh.apply(lambda row: (row['latitude'], row['longitude']), axis = 1)
    #print(loc_neigh.columns)
    #print(neigh.radius_neighbors(tree_loc))
    #print(list(neigh.radius_neighbors(tree_loc).index))
    #print(neigh.radius_neighbors(tree_loc)[1][0])
    #print(df.index)
    loc_neigh = neigh.radius_neighbors(tree_loc)
    print(df.head)
    print(len(loc_neigh[1][0]))
    print(loc_neigh[1][0])
    #full_neigh = df.loc[neigh.radius_neighbors(tree_loc)[1][0]]



    return full_neigh



def draw_sample(year, borough = 'any', zip_code = 'any'):
    table = 'trees_' + str(year)
    if zip_code == 'any':
        zip_list = my_funcs.get_zip_list(table, borough)
        #get list of zips by borough
        zip_code = random.sample(zip_list, 1)[0]
        #draw a random zip from that list

        print(zip_code)
    else:
        zip_code = str(zip_code)
    df = pd.DataFrame(my_funcs.get_data_by_zip(str(zip_code), table), columns = ['tree_id', 'health', 'spc_latin', 'zipcode', 'boroname', 'latitude', 'longitude'])
    df = df.set_index('tree_id')
    df['loc_tuple'] = df.apply(lambda row: (row['latitude'], row['longitude']), axis = 1)
    # df.append(pd.Series(name='same_spc_neighbors'))
    # print(df.head)
    df['same_spc_neighbors'] = np.nan
    df['total_neighbors'] = np.nan
    # df.append(pd.Series(name='total_neighbors'))
    # print(df.head)

    location_df = df.loc[:, ['latitude', 'longitude']]
    # print(location_df.columns)
    # print(location_df.head)

    radius = (500/364286)
    #sets radius to the number of feet in the numerator
    neigh = NearestNeighbors(5, radius)
    neigh.fit(location_df)
    #print('hello')
    #tree_loc = location_df.iloc[:1]

    df['total_neighbors'] = [len(i) for i in neigh.radius_neighbors(location_df, return_distance=False)]

    print(df.shape)
    i = 1
    for spc in list(df['spc_latin']):
        spc_df = df[df['spc_latin'] == spc]
        # print(spc)
        # print("total_df ")
        # print(df.shape)
        # print("spc_df ")
        # print(spc_df.shape)
        spc_loc_df = spc_df.loc[:, ['latitude', 'longitude']]
        #print(spc_loc_df.head)
        spc_neigh = NearestNeighbors(5, radius)
        spc_neigh.fit(spc_loc_df)
        # print(spc_neigh.radius_neighbors(spc_loc_df, return_distance=False)[0])
        # print(len(spc_neigh.radius_neighbors(spc_loc_df, return_distance=False)[0]))
        spc_df['same_spc_neighbors'] = [len(i) for i in spc_neigh.radius_neighbors(spc_loc_df, return_distance=False)]
        # print(spc_df['same_spc_neighbors'].max())
        # print(spc_df.shape)
        #print(spc_df.head)
        #df = pd.concat([df, spc_df], axis=1, sort=False)

        df.update(spc_df)

        #df = df.append(spc_df, sort=False)
        # print(df.shape)
        # print(df.head)
        # i += 1
        # if i == 20:
        #     neigh_df = df.loc[:, ['same_spc_neighbors', 'total_neighbors']]
        #     neigh_tuples = list(neigh_df.itertuples(index=True, name=None))
        #     reverse_tuple = [(i[2], i[1], i[0]) for i in neigh_tuples]
        #     print(neigh_tuples)
        #     print(reverse_tuple)
        #     return




    print(df.head)
    neigh_df = df.loc[:, ['same_spc_neighbors', 'total_neighbors']]
    neigh_tuples = list(neigh_df.itertuples(index=True, name=None))
    reverse_tuple = [(i[2], i[1], i[0]) for i in neigh_tuples]

    my_funcs.update_neighbor_values(reverse_tuple, table=table)
    print('complete')




zip_list = my_funcs.get_zip_list('trees_2015', 'Queens')
#print(zip_list)

#draw_sample('2015', zip_code = zip_list[0])

for zip_code in zip_list:
    if zip_code != zip_list[0]:
        draw_sample('2015', zip_code = zip_code)
        print('zip_code ' + str(zip_code) + 'is complete')
