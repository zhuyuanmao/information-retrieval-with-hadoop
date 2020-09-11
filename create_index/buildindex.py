import sys
import subprocess
import os



input_path = sys.argv[1]        ##Input file directory
output_path = sys.argv[2]       ##Output file directory
current_path = sys.argv[3]      ##IndexBuilder.jar file path


createDir = subprocess.check_output(["bin/hdfs","dfs","-mkdir","/input"])               ##input files into hdfs
copyfile = subprocess.check_output(["bin/hdfs","dfs","-put",input_path,"/input"])
path = input_path.split("/")[-2]
print(path)
path ="/input/"+path

##Hadoop processing part
inverted_index = subprocess.check_output(["bin/hadoop","jar",current_path,"part1.DocTerm",path,"dt.output"])
ptr = subprocess.check_output(["bin/hadoop","jar",current_path,"part1.PosTermRecorder",path,"ptr.output"])
df = subprocess.check_output(["bin/hadoop","jar",current_path,"part1.DocFreqCounter",path,"df.output"])
tf = subprocess.check_output(["bin/hadoop","jar",current_path,"part1.TermFreqCounter",path,"tf.output"])
tfidf = subprocess.check_output(["bin/hadoop","jar",current_path,"part1.TFIDFWeighting",path,"tfw.output","tfidfw.output"])

getfile = subprocess.check_output(["bin/hdfs","dfs","-get","dt.output","dt.output"])
getfile1 = subprocess.check_output(["bin/hdfs","dfs","-get","df.output","df.output"])
getfile2 = subprocess.check_output(["bin/hdfs","dfs","-get","tf.output","tf.output"])
getfile3 = subprocess.check_output(["bin/hdfs","dfs","-get","tfw.output","tfw.output"])
getfile4 = subprocess.check_output(["bin/hdfs","dfs","-get","tfidfw.output","tfidfw.output"])
getfile5 = subprocess.check_output(["bin/hdfs","dfs","-get","ptr.output","ptr.output"])
rm  =  subprocess.check_output(["bin/hdfs","dfs","-rm","-r","/input"])
rm2  =  subprocess.check_output(["bin/hdfs","dfs","-rm","-r","dt.output","df.output","tf.output","tfw.output","tfidfw.output","ptr.output"])


print("\nMapReduce Proccess Ends\n")
print("\nStarting Building Index\n")

##Transfer ouput into a sqlite database.
os.system("cat dt.output/* > dt.tsv")
os.system("cat ptr.output/* > ptr.tsv")
os.system("cat df.output/* > df.tsv")
os.system("cat tf.output/* > tf.tsv")
os.system("cat tfw.output/* > tfw.tsv")
os.system("cat tfidfw.output/* > tfidfw.tsv")
os.system("rm -rf *.output")

query = '''
CREATE TABLE docfreq(
    term TEXT,
    c    INTEGER
);
CREATE TABLE termdoc(
    term TEXT,  
    p    TEXT
);
CREATE TABLE loc(
    term TEXT,
    docid INTEGER,
    pos    TEXT
);
CREATE TABLE termfreq(
    term TEXT,
    docid INTEGER,
    c     INTEGER
);
CREATE TABLE tfidf(
    term TEXT,
    docid INTEGER,
    s   TEXT
);
CREATE TABLE tfw(
    term TEXT,
    docid INTEGER,
    s   TEXT
);
.separator "\t"
.import ptr.tsv loc
.import dt.tsv termdoc
.import df.tsv docfreq
.import tf.tsv termfreq
.import tfw.tsv tfw
.import tfidfw.tsv tfidf
'''

with open("tmp.sql","w") as file:
	file.write(query)
file.close
os.system("cat tmp.sql | sqlite3 "+output_path+ "/index.db")
os.system("rm -f tmp.sql")
os.system("rm -f *.tsv")
print("Index Completed successfully\n")
