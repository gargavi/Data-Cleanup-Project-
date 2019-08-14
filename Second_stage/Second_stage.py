import pymysql as psql
import numpy as np
import pandas as pd
from fuzzywuzzy import fuzz
import re
import time
from sklearn.model_selection import train_test_split

host = '127.0.0.1'
user = 'test'
password = 'secret'

table_dictionary = {
    'medium_data': {'database': 'movies', 'columns': ['origina', 'not_origina', 'id', 'year', 'title']},
    'movies_right':{'database': 'movies', 'columns': ['NONE', 'imdb', 'year', 'title']}
}

#my string.split(",")

#this function creates a dictionary containing the column names and a pandas dataframe with the columns
    #listed in the order that they are listed, theoretically providing ease of comparison
def create_dataframe_dict(table_dictionary):
    dataframe_dict = {}
    def helper_creater(table, dict):
        connection = psql.connect(host, user, password, dict['database'])
        if dict['columns'][0] == 'NONE':

            column_name = (",").join(dict['columns'][1:])
        else:
            column_name = (",").join(dict['columns'])
        dataframe = pd.read_sql_query('select ' + column_name + ' from ' + table, connection)
        return dataframe
    for item in table_dictionary.items():
        dataframe_dict[item[0]] = {'dataframe': helper_creater(item[0], item[1]), 'columns': item[1]['columns']}
    return dataframe_dict


dataframe_list = [ i['dataframe'] for i in create_dataframe_dict(table_dictionary).values()]

#we can create a trianing set and then train the model on that and then deploy it on individual row combinations for determiniation

#first step then becomes to build a training set from the two training sets; we have to first create a function
    #that will take in two rows and modify them into one

def reduce_rows(row1, row2, *args):
    #assume we've been given two rows in pandas dataframe and we want to return a list
        #the *args parameter provides functions that we are going to reduce the individual values of the row by
    assert len(row1) == len(row2) == len(args), "Not gonna work" + str(len(row1)) + str(len(row2)) + str(len(args))
    new_row = []
    for i in range(len(row1)):
        new_row.append(args[i](row1[i], row2[i]))
    return new_row

#this is the function database that we will be adding to to compare different values
def clean_subtract(y, x):
    try:

        a =  int(re.findall('\d+', x)[0]) - y
    except:
        a =  10
    if abs(a) > 100:
        return 10
    return a
def cosine_similarity(x,y):
    return fuzz.ratio(x, y)

def training_dataframe(df, de, column):
    #this is the function that will be ideally taking two dataframes and combined the applicable rows into one by row them
    #df should be the dataframe that contains the match column
    column_list = []
    for i in range(len(df.columns) -3):
        column_list.append('Distance' + str(i))
    column_list.append('Result')

    total_rows = []
    for i in range(len(df.index)):
        match = df.iloc[i][0]
        if match:
            print('match: ' + match)
            match_list = df.iloc[i][0].split(", ")
            for match in match_list:
                row1 = de.iloc[de[de[column] == match].index.tolist()[0]][1:]
                row2 = df.iloc[i][3:]
                new_row = reduce_rows(row1, row2, clean_subtract, cosine_similarity)
                new_row.append(1)
                total_rows.append(new_row)
        not_match = df.iloc[i][1]
        if not_match:
            print('Not Match: ' + not_match)
            not_match_list = df.iloc[i][1].split(", ")
            for match in not_match_list:
                row1 = de.iloc[de[de[column] == match].index.tolist()[0]][1:]
                row2 = df.iloc[i][3:]
                new_row = reduce_rows(row1, row2, clean_subtract, cosine_similarity)
                new_row.append(0)
                total_rows.append(new_row)

    new_df = pd.DataFrame(total_rows,columns=column_list)
    return new_df


data = training_dataframe(dataframe_list[0], dataframe_list[1], 'imdb')

X_train, X_test, y_train, y_test  = train_test_split(
    data.drop('Result', axis = 1), data['Result'], test_size = .2, random_state = 101)

from sklearn.linear_model import LogisticRegression
logmodel = LogisticRegression()
logmodel.fit(X_train, y_train)
predictions = logmodel.predict(X_test)


from sklearn.metrics import classification_report
print(logmodel.coef_)
print(classification_report(predictions, y_test))
input()

def deployment(df, de, column):
    #goal of this is to deploy the model we just trained onto the combination and arrive at a conclusion for any two rows
    all_rows = []
    t0 = time.time()
    n = 0
    for i in range(len(df.index)):
        if i % 1000 == 0:
            print(i)
            print (time.time() - t0)

        for j in range(len(de.index)):
            try:
                row1 = df.iloc[i][3:]
                row2 = de.iloc[j][1:]
                new_row = np.asarray(reduce_rows(row1, row2, clean_subtract, cosine_similarity))
                if logmodel.predict_proba(new_row.reshape(1, -1))[0][0] > .75:
                    all_rows.append(new_row)
            except Exception as e:
                print(e)
                n+=1
    print(n)
deployment(dataframe_list[0], dataframe_list[1], 'imdb')






#predict = logmodel.predict(total_list)
#        for i in range(len(total_list)):
 #           print('X%s, Predicted = %s' % (total_list[i], predict[i]))
  #          input()















