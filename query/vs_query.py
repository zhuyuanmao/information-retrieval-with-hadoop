import sys 
import sqlite3 
from math import log
import operator 
import shlex
from Stemmer import *

NUM_COMMAND_LINE_ARGS = 4

# global variables, manipulated and used by varies functions (e.g, procedural programming technique)
scores = ()
index_location = ""
query = ""
scores_y_n = ''
k = 0

#phrase_info = {'': [posting_list, docfreq, raw_data]}

phrase_info = {}

query_weight = {}

N = 0 # number of documents in collection


def init():
	global conn, c, scores, N

	conn = sqlite3.connect(index_location)
	c = conn.cursor()

	c.execute("select count(distinct docid) from loc")
	N = int(c.fetchone()[0])

	terms = get_terms()

	for term in terms:
		if(is_phrase(term)):
			# pre-fetching info for phrases for faster processing

			counter = 1
			phrase_terms = term.split()
			table = ""
			prev_table = "loc" + str(counter)

			select = "*"
			frm = "loc as {}".format(prev_table)
			where = "{}.term='{}'".format(prev_table, phrase_terms[0])

			for pt in phrase_terms[1:]:
				counter += 1

				table = "loc" + str(counter)

				frm += ", loc as " + table
				where += " AND {}.term='{}' AND {}.docid={}.docid".format(
					table, pt, prev_table, table)

				prev_table = table
			
			query = "SELECT {}\nFROM {}\nWHERE {}".format(select, frm, where)
			c.execute(query)
			rows = c.fetchall()

			for row in rows:
				tf = get_phrase_info(row)
				if(tf > 0):
					docID = int(row[1])

					if(term in phrase_info):
						phrase_info[term][0].append([docID, tf])
						phrase_info[term][1] += 1
					else:
						phrase_info[term] = [[[docID, tf]], 1]

		idf = get_idf(term)
		w_tf = get_wtf(term)
		query_weight[term] = w_tf * idf

def cosine_score():
	global scores

	terms = get_terms()
	scores_dic = {}

	for term in terms:
		wtq = query_weight[term]
		posting_list = get_posting_list(term) 
		for docID in posting_list:
			wtd = get_wtd(term, docID)
			scores_dic[docID] = wtq * wtd
	
	scores = sorted(scores_dic.items(), key=operator.itemgetter(1), reverse=True)

def print_top_k_docs():
	i = 0
	for docScore in scores:
		if(i >= k):
			break
		if(scores_y_n == 'y'):
			print("docID: {}\tscore: {:.2f}".format(docScore[0], docScore[1]))
		else:
			print("docID: {}".format(docScore[0]))
		i += 1


# helper methods

def get_list(string_list):
	return [int(e) for e in string_list[2:-2].replace(',', '').split()]

def get_all_pos(row):
	all_pos = []
	i = 2
	while(i < len(row)):
		pos = get_list(row[i])
		all_pos.append(pos)
		i += 3

	return all_pos

def get_phrase_info(row):
	valid_row = True
	tf = 0

	all_pos = get_all_pos(row)
	size = len(all_pos)
	pos1 = all_pos[0]

	i = 0
	while(i < len(pos1)):
		valid_row = True
		p1 = pos1[i]

		k = 1
		while(k < size):
			pos2 = all_pos[k]
			j = 0

			while(j < len(pos2)):
				p2 = pos2[j]
				if(p1 - p2 > 1):
					break
				j += 1

			if(j >= len(pos2)):
				valid_row = False
				break
			k += 1

		if(valid_row):
			tf += 1

		i += 1
	return tf

def is_phrase(term):
	return len(term.split()) > 1

def get_length():
	length = []
	return length

def get_terms():
	split = shlex.split(query)

	p = PorterStemmer()
	outputList = []

	for word in split:
		if(len(word.split()) > 1):
			sp = word.split()
			o = []
			for s in sp: 
				s = s.lower()
				output = p.stem(s, 0,len(s)-1)
				o.append(output)
			output = ' '.join(o)

		else:
			word = word.lower()
			output = p.stem(word, 0,len(word)-1)

		outputList.append(output)
	return outputList

def get_posting_list(term):
	posting_list = []

	if(not is_phrase(term)):
		query = '''SELECT p
		    		 FROM termdoc
					 WHERE term='{}';
					 '''.format(term)

		c.execute(query)
		query_result = c.fetchone()

		if(query_result):
			posting_list = get_list(query_result[0])
	else:
		posting_list = []
		pos_info = phrase_info[term][0]
		for docID_tf in pos_info:
			posting_list.append(int(docID_tf[0]))


	return posting_list

def get_wtd(term, docID):
	wtd = 0.0

	if(not is_phrase(term)):
		c.execute('''SELECT s
				 FROM tfidf
				 where term='{}' and docid='{}';
				 '''.format(term, docID))
		query_result = c.fetchone()

		if(query_result):
			wtd = float(query_result[0])

	else:
		tf = -1
		df = 0
		posting_list = phrase_info[term][0]

		df = phrase_info[term][1]

		for docID_tf in posting_list:
			if(docID_tf[0] == docID):
				tf = docID_tf[1]
		wtf = tf*df

	return wtd

def get_doc_freq(term):
	df = 0.0

	if(not is_phrase(term)):
		c.execute('''SELECT c 
				 FROM docfreq 
				 WHERE term='{}';
				 '''.format(term))
		query_result = c.fetchone()

		if(query_result):
			df = float(query_result[0])
	else:
		df = phrase_info[term][1]
	return df

def get_idf(term):
	idf = 0.0

	df = get_doc_freq(term)
	if(df != 0):
		idf = log(N / df, 10)
	return idf

def get_wtf(term):
	tf = get_terms().count(term)
	return 1 + log(tf, 10)


if __name__ == '__main__':

	if(len(sys.argv) < NUM_COMMAND_LINE_ARGS):
		print("usage: ./vs_query [index location] [k] [scores] [term_1] [term_2] ... [term_n]")
		exit(0)
	
	index_location = sys.argv[1]
	k = int(sys.argv[2])
	scores_y_n = sys.argv[3]
	query = ' '.join(sys.argv[4:])

	init()
	cosine_score()
	print_top_k_docs()
	conn.close()

