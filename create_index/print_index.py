import sqlite3
import sys
index = sys.argv[1]

query = "select df.term,df.c, tf.docid, tf.c, l.pos from termfreq tf, docfreq df, termdoc td,loc l where td.term = tf.term and td.term = l.term and l.docid = tf.docid and td.term =df.term;"
try:
    conn = sqlite3.connect(index)
except Exception as e:
    print("error: Invalid Path")

c = conn.cursor()
last_term = ""
for t in c.execute(query):
    term = t[0]
    if (term != last_term):
        print("\n",term,",",t[1],":")

    last_term = term
    print("\t",t[2],",",t[3],":",t[4])

    
    
    

