[Assignment specs](https://sites.ualberta.ca/~denilson/cmput397-winter-2019-assignment-1.html)

# Don't Forget to

1. Submit the URL of your repository on eClass **as soon as you read this line**! 
1. Disclose all sources you consult
1. Spend time on your design; you need to break down the tasks so that all team members can work in parallel
1. Meet with the TA about your group progress and to ask for advice 
1. Test your code
1. Document any assumptions and design decisions
1. Add clear execution instructions

# Git Workflow: Feature Branch Workflow:

1. Identify a specific functionality
2. Create an issue for this functionality
3. Assign this issue to someone (optional)
4. Choose an appropriate label for this issue.
5. To work on this issue, create a branch and develop a solution
6. Commit your changes with the message 'close #<issue number -> see Issues>
7. Merge the branch with master through a pull request
8. Development does not happen in master, only merges.


 
 # How To Compile
 
   # Assumption before Building Index
     
   
   **Installing and Setting up Hadoop**

  1. Download Hadoop version 2.9.2 (.tar.gz folder)
   - http://apache.forsale.plus/hadoop/common/hadoop-2.9.2/
 
  2. Once stored on a desired directory, right click and choose extract all

  3. In the file hadoop-2.9.2/etc/hadoop/hadoop-env.sh edit the following lines to be as follows:

   - line 25: __export JAVA_HOME=/usr/lib/jvm/java-1.9.0-openjdk-amd64__
   - line 39: __export HADOOP_CLASSPATH=/usr/lib/jvm/java-1.9.0-openjdk-amd64/jre/lib/tools.jar__ (with indentation kept as in original)
  - line 41: __export HADOOP_CLASSPATH=/usr/lib/jvm/java-1.9.0-openjdk-amd64/jre/lib/tools.jar__ (with indentation kept as in    original)
 
  4. Inside the files  	*italics*hadoop-2.9.2/etc/hadoop/core-site.xml*italics* and hadoop-2.9.2/etc/hadoop/hdfs-site.xml, add   configuration as shown on below link
 - http://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-common/SingleCluster.html
 
  5. Have Been Setting up a Single Node Cluster.
  
  6. Have Been Format the filesystem.
            
            $ bin/hdfs namenode -format
  
  7. Have Been Start NameNode daemon and DataNode daemon.
       
            $ sbin/start-dfs.sh
   
   
 
 
   # Proceduere 1: Building Index
 
   1. In the directory: cmput397-w19-proj1-yuanmaoChris/create_index/ 
  
     $./makefile.sh 
  
   2. Check IndexBuilder.jar (produced by **makefile.sh**) if it exists, if so go to the hadoop homepath.
  
     $ python3  /path/of/buildindex.py /input/path /output/path /path/of/IndexBuilder.jar
   
   3. The final index file called  **index.db**
   
   
   # Proceduere 2: Printing Out Index
    
   1. In the directory: cmput397-w19-proj1-yuanmaoChris/create_index/ 
    
    $ python3 print_index.py /path/to/index.db
    
   # Proceduere 3: Query
      
        $ python3 vs_query.py [index location] [k] [scores] [term_1] [term_2] ... [term_n]
        
     
       
   
  
  # Normalization
  
   we are using The Porter stemming algorithm to normalize the data.
      
       step1() gets rid of plurals and -ed or -ing. e.g. caresses  ->  caress; ponies    ->  poni

       step2() turns terminal y to i when there is another vowel in the stem.
       
       step3() maps double suffices to single ones. so -ization
       
       step4() deals with -ic-, -full, -ness etc. similar strategy to step3.
       
       step5() takes off -ant, -ence etc., 
       
       step6() removes a final -e 
  
  # The Files Submited in Project 1:
  In the **create_index** directory
         
         buildindex.py   The python is used for building index.db
      
         createIndex     The directory contains the Maven framework
      
         makefile.sh     The compile java file to jar file which is used for hadoop
   
         print_index.py  Print out index.
         
         runtests.sh     run test.

   In the **query** directory
 
    
        vs_query.py       The python file is used for search the index.


  
  
  # Reference
  
  MapReduce:https://github.com/kshitijkhurana3010/Information-retrieval-unstructured-text-Hadoop-MapReduce/blob/master/Search.java
  
  Index Construction: https://courses.cs.ut.ee/MTAT.08.011/2013_spring/uploads/Main/L5_InformationRetrieval.pdf
  
  Stemmer: https://tartarus.org/martin/PorterStemmer/
