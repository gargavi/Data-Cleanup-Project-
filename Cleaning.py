import csv

# read tab-delimited file
with open('title.tsv','rt') as fin:
    cr = csv.reader(fin, delimiter='\t')
    cv = iter(cr)
    i = 0
    filecontents = []
    while i < 100000:
        filecontents.append(next(cv))
        i += 1

# write comma-delimited file (comma is the default delimiter)
with open('movies1.csv','wt') as fou:
    cw = csv.writer(fou, quotechar='', quoting=csv.QUOTE_NONE, escapechar = '\\')
    cw.writerows(filecontents)