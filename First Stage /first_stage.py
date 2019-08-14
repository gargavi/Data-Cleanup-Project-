
from fuzzywuzzy import fuzz
from sqlalchemy import create_engine

import pandas as pd
import pymysql as psql

#all the user defined information sits in this chunk of code --> what the user manipulates in order to get a
    #final table in mysql that is in the database given with the name provided using the dictionary of tables given

    #if we wanted to include every column we would have to issue that below, because it still matters how each column
        #is paired against the other
            #Question: Is there a way that I can be given two tables and myself find the relevant relationships between
                #the columns
            #Most definitely can be just given two data sets and train it, but it would be much more accurate to give
                #it prominent relationships and it uses those first
                #possible to do that first and then add a layer on top that compares the others

    #need to add asserts and raise Errors to make sure code 'runs' under incorrect data input
    #develop a better enrich function and create two new movies csv databases to use for testing
    #figure out how to do remote server and try accessing the OCF database that I have with the school

host = '127.0.0.1'
user = 'root'
password = 'ILoveNaan2Much'
final_database_dest = 'random'
final_table_name = 'update_movies'
table_dictionaries = {
    #format is table_name: [database_name, column_num1, column_num2 ...]
    #the table in front recieves highest priority in terms of sorting; it will be the one who will dominant should a match
        #be found and will continue all the way back; the column_names should be listed in relative order of comparision
        #i.e the second item of all lists will be compared, the third item of all lists will be compared etc...
    'movies_right': ['movies', 'title'],
    'movies_wrong': ['movies', 'titlN'],
    #'newmovies': ['movies', 'title']
}


#creates the engine for the final database
if host != '127.0.0.1':
    conn = create_engine('mysql+pymysql://' + user + ':' + password + '@' + host + '/' + final_database_dest)
else:
    conn = create_engine('mysql+pymysql://' + user + ':' + password + '@localhost/' + final_database_dest)


#creates a dictionary with the following format:
    #key = name of the table
    #value = list with the corresponding dataframe at index 0, and the columns at indexes 1 - however much
def create_dataframe_list(table_dictionary):
    table_info = {}
    # creates a pandas dataframe using the name of the database and the name of the table
    def create_dataframe(table, database):
        connection = psql.connect(host, user, password, database)
        dataframe = pd.read_sql_query('select * from ' + table, connection)
        return dataframe

    for item in table_dictionary.items():
        dataframe = create_dataframe(item[0], item[1][0])
        table_info[item[0]] = [dataframe] + item[1][1:]
    return table_info


def transform_name(name_list, dataframe):
    #transform the list of names to the corresponding keys in the dataframe
    print(name_list)
    keys = dataframe.keys()
    print(keys)
    def finder(val, keys):
        for i in range(len(keys)):
            if val == keys[i]:
                return i
        print('GACK:', val)
        input()
    return [finder(val, keys) for val in name_list]


def parsed_data_set(table1, table2, function, enrich_func):

    def prepare_dataframe(dataframe, list1, list2):
        change_dict = {}
        for i in range(len(list1)):
            change_dict[list2[i]] = list1[i]
        return dataframe.rename(change_dict, axis='columns')

    combined_dataframe = pd.DataFrame()
    df = table1[0]
    de = table2[0]
    total_j = []
    column1 = transform_name(table1[1:], df)
    column2 = transform_name(table2[1:], de)
    failures = 0
    successes = 0
    #two scenarios, either the row in table1 matches with row(s) in table 2 or it doesn't
        #if it doesn't match, it simply gets appended to the combined_dataframe
        #if it matches with a subset of results, all them go through the enriching process and it returns a single row
            #which gets appended to the combined dataframe
        #for every value in dataset 2 which isn't a duplicate, it needs to be appended to the new dataframe

    for i in range(len(df.index)):
        to_be_enriched = []
        for j in range(len(de.index)):
            if function([[df.iloc[i, column1[x]], de.iloc[j, column2[x]]] for x in range(len(column1))]):
                to_be_enriched.append(j)
                total_j.append(j)
        if len(to_be_enriched) == 0:
            combined_dataframe = combined_dataframe.append(df.iloc[i, :])
        else:
            to_be_enriched  = [df.iloc[i, :]] + to_be_enriched
            combined_dataframe = combined_dataframe.append(enrich_func(to_be_enriched))
    d  = de.drop(total_j)
    d = prepare_dataframe(d, table1[1:], table2[1:])
    print('DATABASE: Completed')
    input()
    return [combined_dataframe.append(d, sort = True)] + table1[1:]


def parse_complete(table_dictionaries, function ,enrich_function):
    table_dict = create_dataframe_list(table_dictionaries)
    iter_dict = iter(table_dict.values())
    new_set = parsed_data_set(next(iter_dict), next(iter_dict), function, enrich_function)
    for i in range(len(table_dict.keys()) -2):
        new_set = parsed_data_set(new_set, next(iter_dict), function, enrich_function)
    return new_set[0]


def parser_func(cutoff):
    def parse(values):
        #given input in form [[x,y], [z,y], [g,h] ]
        for val in values:
            if fuzz.ratio(str(val[0]), str(val[1])) > cutoff:
                print(str(val[0]), str(val[1]), 'identified as close')
                return True
        return False
    return parse

def enrich_func(rows):
    return rows[0]
#things to consider about the words; length, similarity by letter, if one contains the other,


def send_to_sql(dataframe, name, conn):
        dataframe.to_sql(name, conn, if_exists = 'replace')
        print('done')

send_to_sql(parse_complete(table_dictionaries, parser_func(72), enrich_func), final_table_name, conn)
