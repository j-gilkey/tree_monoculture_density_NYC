import mysql.connector
import config

#set database name
DB_NAME = 'birds_trees_etc'

#create connection
cnx = mysql.connector .connect(
    host = config.host,
    user = config.user,
    passwd = config.password,
    database = DB_NAME,
    use_pure=True
)

#start cursor
cursor = cnx.cursor()


def insert_tree_1995(tree_tuples):
    add_tree = ("""INSERT INTO trees_1995
               (tree_id, health, spc_latin, spc_common, address, zipcode, boroname, lat, lng)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);""")

    cursor.execute(add_tree, tree_tuples)
    cnx.commit()


def insert_tree_2005(tree_tuples):
    add_tree = ("""INSERT INTO trees_2005
               (tree_id, health, spc_latin, spc_common, address, zipcode, boroname, lat, lng)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);""")

    cursor.execute(add_tree, tree_tuples)
    cnx.commit()

def insert_tree_2015(tree_tuples):
    add_tree = ("""INSERT INTO trees_2015
               (tree_id, health, spc_latin, spc_common, address, zipcode, boroname, lat, lng)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);""")

    cursor.execute(add_tree, tree_tuples)
    cnx.commit()

def insert_zip(zip_tuple):
    add_zip = ("""INSERT INTO zip_demographics
               (zip_code, median_home_value, median_household_income, land_area_in_sqmi)
               VALUES (%s, %s, %s, %s);""")

    cursor.execute(add_zip, zip_tuple)
    cnx.commit()

def get_zip_info():

    get_zip  = ('(SELECT * FROM zip_demographics)')

    cursor.execute(get_zip)
    return cursor.fetchall()

def get_zip_list(table, borough = 'any'):
    #takes in a database name from the list ['trees_2005', 'trees_2015', 'trees_1995'] and a borough and returns a full zip list
    if borough == 'any':
        get_list = ('(SELECT zipcode FROM ' + table + ' WHERE total_neighbors IS NULL GROUP BY zipcode)')
    else:
        get_list = ('(SELECT zipcode FROM ' + table + ' WHERE total_neighbors IS NULL AND boroname = "'+ borough +'" GROUP BY zipcode)')

    cursor.execute(get_list)
    return list(map(lambda x: x[0],cursor.fetchall()))

#print(get_zip_list('trees_2005', 'Queens'))

def get_data_by_zip(zip_code, table):
    get_data = ('(SELECT tree_id, health, spc_latin, zipcode, boroname, lat, lng FROM ' + table + ' WHERE zipcode = "' + zip_code + '")')
    cursor.execute(get_data)
    return cursor.fetchall()


def get_monoculture_data():
    get_data = ('(SELECT tree_id, health, spc_latin, zipcode, boroname, lat, lng, total_neighbors, same_spc_neighbors FROM birds_trees_etc.trees_2015 WHERE total_neighbors > 0)')
    cursor.execute(get_data)
    return cursor.fetchall()


def update_neighbor_values(tuple_list, table):
    update_vals = ('UPDATE ' + table +
                """
                SET total_neighbors = %s,
                    same_spc_neighbors = %s
                WHERE tree_id = %s;
                """ )
    cursor.executemany(update_vals, tuple_list)
    cnx.commit()

def get_all_trees_with_health_agg():

    get_tree = ('''(SELECT zipcode
                    		,boroname
                            ,CASE
                                WHEN health = 'Excellent' THEN 1
                                WHEN health = 'Good' THEN 1
                                WHEN health = 'Planting S' THEN 1
                                ELSE 0
                            END AS healthy
                            ,'1995' AS year
                    		,count(tree_id) AS tree_count
                    FROM trees_1995
                    GROUP BY 1
                    		,2
                    		,3
                    		,4
                    )

                    UNION ALL

                    (SELECT zipcode
                    		,boroname
                    		,CASE
                                WHEN health = 'Excellent' THEN 1
                                WHEN health = 'Good' THEN 1
                                ELSE 0
                            END AS healthy
                            ,'2005' AS year
                    		,count(tree_id)  AS tree_count
                    FROM trees_2005
                    GROUP BY 1
                    		,2
                    		,3
                    		,4
                    )

                    UNION ALL

                    (SELECT zipcode
                    		,boroname
                    		,CASE
                                WHEN health = 'Fair' THEN 1
                                WHEN health = 'Good' THEN 1
                                ELSE 0
                            END AS healthy
                            ,'2015' AS year
                    		,count(tree_id)  AS tree_count
                    FROM trees_2015
                    GROUP BY 1
                    		,2
                    		,3
                    		,4
                    )''')

    cursor.execute(get_tree)
    return cursor.fetchall()


def get_distinct_trees_by_zip():

    get_tree = ('''(SELECT zipcode
                    		,boroname
                            ,'1995' AS year
                    		,count(tree_id) AS tree_count
                            ,count(DISTINCT spc_latin) AS distinct_species
                    FROM trees_1995
                    GROUP BY 1
                    		,2
                    		,3
                    )

                    UNION ALL

                    (SELECT zipcode
                    		,boroname
                            ,'2005' AS year
                    		,count(tree_id)  AS tree_count
                            ,count(DISTINCT spc_latin) AS distinct_species
                    FROM trees_2005
                    GROUP BY 1
                    		,2
                    		,3
                    )

                    UNION ALL

                    (SELECT zipcode
                    		,boroname
                            ,'2015' AS year
                    		,count(tree_id)  AS tree_count
                            ,count(DISTINCT spc_latin) AS distinct_species
                    FROM trees_2015
                    GROUP BY 1
                    		,2
                    		,3
                    )''')

    cursor.execute(get_tree)
    return cursor.fetchall()
