import csv 
import random 
import string 



table = open('movies.csv')

read_t = csv.reader(table, delimiter = ',')



def incorrect_mod(name):
	name = list(name)
	for i in range(len(name)):
		if random.randint(0,5) == 1:
			name[i] = random.choice(string.ascii_letters)
	return "".join(name)



with open('movies_right.csv', mode = 'w') as wrong_movies:
	wrong_writer = csv.writer(wrong_movies, delimiter = ',', quotechar = '"')
	for row in read_t:
		wrong_writer.writerow(row)

