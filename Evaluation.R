
library(RMariaDB)

library(tictoc)

library(reticulate)
library(dplyr)





tic()


con <- dbConnect(drv = RMariaDB::MariaDB(), username = "root",password = "password", host="host", port = 3306)


SQL<-dbSendQuery(con, paste(" select *  from waves.type4_assestid_null "))

Transactions_outdegree_SQL <- dbFetch(SQL)
toc()

edege_14<-read.csv("edege_14.csv",header=TRUE, quote="", sep=",")

ohn_quote<-as.data.frame(sapply(edege_14, function(x) gsub("\"", "", x)))
dataframecsv<-data.frame (sender=ohn_quote$from,recipient=ohn_quote$to,amount=ohn_quote$weight)

write.csv(dataframecsv,  "dataframecsv14.csv", row.names = F, na="", sep=",",quote=F, append=F)




#get all data
tic()
use_python("C:\Users\PC\AppData\Local\Programs\Python\Python37")

is_python_available <- py_available()
#paste("Is Python Available?", is_python_available)


uri = "bolt://localhost:7687"

neo4j <-import("neo4j", convert = FALSE)
token <- neo4j$basic_auth("neo4j","blockla5er")
driver <- neo4j$GraphDatabase$driver(uri, auth=token)
the_session <- driver$session()
tx <- the_session$begin_transaction()
record <- tx$run("MATCH (p1:Sender)-[w:senden]->(r:Sender)  RETURN p1.name as from,w.amount as weight,r.name as to")
record_data <- record$data()
toc()


#List most sender


tic()
use_python("C:\Users\PC\AppData\Local\Programs\Python\Python37")

is_python_available <- py_available()
#paste("Is Python Available?", is_python_available)


uri = "bolt://localhost:7687"

neo4j <-import("neo4j", convert = FALSE)
token <- neo4j$basic_auth("neo4j","blockla5er")
driver <- neo4j$GraphDatabase$driver(uri, auth=token)
the_session <- driver$session()
tx <- the_session$begin_transaction()
record <- tx$run("MATCH (user:Sender )-[w:senden]->(recipient:Sender) with count(w)as count, user as user  return user.name, count  order by  count desc  limit 10")
record_data <- record$data()
toc()
dat<-py_to_r(record_data)

#convertir list to dataframe
ed <- data.frame(matrix(unlist(dat), ncol = max(lengths(dat)), byrow = TRUE))





tic()

con <- dbConnect(drv = RMariaDB::MariaDB(), username = "root",password = "password", host="host", port = 3306)


SQL<-dbSendQuery(con, paste("   select COUNT(*) AS sumamount, sender  Sender FROM waves.type4_assestid_null  GROUP BY  sender  order by  sumamount desc limit 10 "))

Transactions_outdegree_SQL <- dbFetch(SQL)
toc()










# get data with given adress


tic()

con <- dbConnect(drv = RMariaDB::MariaDB(), username = "root",password = "password", host="host", port = 3306)


SQL<-dbSendQuery(con, paste(" select sum(amount) AS sumamount, sender  Sender FROM waves.type4_assestid_null",
"  GROUP BY  sender  order by  sumamount desc  limit 10  "))

Transactions_outdegree_SQL_most10 <- dbFetch(SQL)
toc()



#get most 10 recipient

tic()
use_python("C:\Users\PC\AppData\Local\Programs\Python\Python37")

is_python_available <- py_available()
#paste("Is Python Available?", is_python_available)


uri = "bolt://localhost:7687"

neo4j <-import("neo4j", convert = FALSE)
token <- neo4j$basic_auth("neo4j","blockla5er")
driver <- neo4j$GraphDatabase$driver(uri, auth=token)
the_session <- driver$session()
tx <- the_session$begin_transaction()
record <- tx$run("MATCH (user:Sender )-[w:senden]->(recipient) with count(w)as count, recipient as user  return user.name, count  order by  count desc  limit 10")
record_data <- record$data()
toc()
dat<-py_to_r(record_data)

#convertir list to dataframe
ed <- data.frame(matrix(unlist(dat), ncol = max(lengths(dat)), byrow = TRUE))





tic()

con <- dbConnect(drv = RMariaDB::MariaDB(), username = "root",password = "password", host="host", port = 3306)




SQL<-dbSendQuery(con, paste(" select sender as sender ,COUNT(*) as count, recipient as recipient from waves.type4_assestid_null where recipient in  (SELECT   recipient FROM ( ( select COUNT(*) AS count, recipient as  recipient FROM waves.type4_assestid_null  GROUP BY  recipient  order by  count desc  limit 10 )  sum) ) group by sender, recipient order by count desc limit 10 "))

Transactions_outdegree_SQL <- dbFetch(SQL)
toc()





#List for each of the  top 10 recipient from where received the most transactions.




tic()
use_python("C:\Users\PC\AppData\Local\Programs\Python\Python37")

is_python_available <- py_available()
#paste("Is Python Available?", is_python_available)


uri = "bolt://localhost:7687"

neo4j <-import("neo4j", convert = FALSE)
token <- neo4j$basic_auth("neo4j","blockla5er")
driver <- neo4j$GraphDatabase$driver(uri, auth=token)
the_session <- driver$session()
tx <- the_session$begin_transaction()
record <- tx$run("MATCH (user:Sender)-[w:senden]->(recipient) WITH count(w) AS mutualFriends, recipient as r, user as us ORDER BY mutualFriends DESC LIMIT 10 RETURN r.name, mutualFriends,us.name")
record_data <- record$data()
toc()
dat<-py_to_r(record_data)

#convertir list to dataframe
ed <- data.frame(matrix(unlist(dat), ncol = max(lengths(dat)), byrow = TRUE))



tic()

con <- dbConnect(drv = RMariaDB::MariaDB(), username = "root",password = "password", host="host", port = 3306)


SQL<-dbSendQuery(con, paste(" select sender as sender ,COUNT(*) as count, recipient as recipient from waves.type4_assestid_null where recipient in  (SELECT   recipient FROM ( ( select COUNT(*) AS count, recipient as  recipient FROM waves.type4_assestid_null  GROUP BY  recipient  order by  count desc  limit 10 )  sum) ) group by sender, recipient order by count desc limit 10 "))

Transactions_outdegree_SQL <- dbFetch(SQL)
toc()






tic()
use_python("C:\Users\PC\AppData\Local\Programs\Python\Python37")

is_python_available <- py_available()
#paste("Is Python Available?", is_python_available)


uri = "bolt://localhost:7687"

neo4j <-import("neo4j", convert = FALSE)
token <- neo4j$basic_auth("neo4j","blockla5er")
driver <- neo4j$GraphDatabase$driver(uri, auth=token)
the_session <- driver$session()
tx <- the_session$begin_transaction()
record <- tx$run("MATCH (user:Sender)-[:senden*1..2]->(recipient) WHERE recipient <> user RETURN recipient.name")
record_data <- record$data()
toc()
dat<-py_to_r(record_data)

#convertir list to dataframe
ed <- data.frame(matrix(unlist(dat), ncol = max(lengths(dat)), byrow = TRUE))






# get tramsaction depth 3


tic()

con <- dbConnect(drv = RMariaDB::MariaDB(), username = "root",password = "password", host="host", port = 3306)

SQL<-dbSendQuery(con, paste(" select distinct recipient as recipi from waves.type4_assestid_null where sender in  (select distinct recipient from  waves.type4_assestid_null where sender in (select recipient from   (select distinct recipient  from waves.type4_assestid_null limit 10000) as v2 ))"))

Transactions_outdegree_SQL <- dbFetch(SQL)
toc()





tic()
use_python("C:\Users\PC\AppData\Local\Programs\Python\Python37")

is_python_available <- py_available()
#paste("Is Python Available?", is_python_available)


uri = "bolt://localhost:7687"

neo4j <-import("neo4j", convert = FALSE)
token <- neo4j$basic_auth("neo4j","blockla5er")
driver <- neo4j$GraphDatabase$driver(uri, auth=token)
the_session <- driver$session()
tx <- the_session$begin_transaction()
record <- tx$run("MATCH (n)
WITH distinct n
limit 10
match (n)-[:senden*1..3]->(recipient)
WHERE recipient <> n
RETURN recipient.name, n.name")
record_data <- record$data()
toc()
dat<-py_to_r(record_data)

#convertir list to dataframe
ed <- data.frame(matrix(unlist(dat), ncol = max(lengths(dat)), byrow = TRUE))



# Find the most received recipient for each of the top 10 sender





tic()

con <- dbConnect(drv = RMariaDB::MariaDB(), username = "root",password = "password", host="host", port = 3306)


SQL<-dbSendQuery(con, paste("select recipient, count(recipient) as sum ,  sender  from  waves.type4_assestid_null where sender in   (select sender from   (select count(amount) AS sumamount,  sender from  waves.type4_assestid_null GROUP BY  sender order by  sumamount desc limit 10) as v2 ) GROUP BY  recipient, sender order by  sum desc limit 10"))

Transactions_outdegree_SQL <- dbFetch(SQL)
toc()




tic()
use_python("C:\Users\PC\AppData\Local\Programs\Python\Python37")

is_python_available <- py_available()
#paste("Is Python Available?", is_python_available)


uri = "bolt://localhost:7687"

neo4j <-import("neo4j", convert = FALSE)
token <- neo4j$basic_auth("neo4j","blockla5er")
driver <- neo4j$GraphDatabase$driver(uri, auth=token)
the_session <- driver$session()
tx <- the_session$begin_transaction()
record <- tx$run("MATCH (user:Sender )-[w:senden]->(recipient:Sender) with count(w)as count, user as user   order by  count desc  limit 10  MATCH (user )-[w:senden]->(r:Sender) with count(w)as count1, user as user , r as r   order by  count1 desc  limit 10 return user.name,r.name, count1")
record_data <- record$data()
toc()
dat<-py_to_r(record_data)

#convertir list to dataframe
ed <- data.frame(matrix(unlist(dat), ncol = max(lengths(dat)), byrow = TRUE))




#List the top 10 recipients based on  volume  of transactions.

tic()

con <- dbConnect(drv = RMariaDB::MariaDB(), username = "root",password = "password", host="host", port = 3306)



SQL<-dbSendQuery(con, paste("select recipient , sum(amount) as sum  from  waves.type4_assestid_null GROUP BY  recipient  order by  sum desc limit 10"))

Transactions_outdegree_SQL <- dbFetch(SQL)
toc()



tic()
use_python("C:\Users\PC\AppData\Local\Programs\Python\Python37")

is_python_available <- py_available()
#paste("Is Python Available?", is_python_available)


uri = "bolt://localhost:7687"

neo4j <-import("neo4j", convert = FALSE)
token <- neo4j$basic_auth("neo4j","blockla5er")
driver <- neo4j$GraphDatabase$driver(uri, auth=token)
the_session <- driver$session()
tx <- the_session$begin_transaction()
record <- tx$run("MATCH (p1:Sender)-[w:senden]->(r:Sender) with  p1 as p,SUM(toInteger(w.amount)) as sum   ORDER BY sum DESC limit 10 return p.name ,sum")
record_data <- record$data()
toc()
dat<-py_to_r(record_data)

#convertir list to dataframe
ed <- data.frame(matrix(unlist(dat), ncol = max(lengths(dat)), byrow = TRUE))




#List the top 10 senders based on volume of transactions.


tic()

con <- dbConnect(drv = RMariaDB::MariaDB(), username = "root",password = "password", host="host", port = 3306)



SQL<-dbSendQuery(con, paste("select sender , sum(amount) as sum  from  waves.type4_assestid_null GROUP BY  sender  order by  sum desc limit 10"))

Transactions_outdegree_SQL <- dbFetch(SQL)
toc()



tic()
use_python("C:\Users\PC\AppData\Local\Programs\Python\Python37")

is_python_available <- py_available()
#paste("Is Python Available?", is_python_available)


uri = "bolt://localhost:7687"

neo4j <-import("neo4j", convert = FALSE)
token <- neo4j$basic_auth("neo4j","blockchain")
driver <- neo4j$GraphDatabase$driver(uri, auth=token)
the_session <- driver$session()
tx <- the_session$begin_transaction()
record <- tx$run("MATCH (p1:Sender)-[w:senden]->(r:Recipient) with  r as p , SUM(toInteger(w.weight)) as sum   ORDER BY sum DESC limit 10 return p.num,sum")
record_data <- record$data()
toc()
dat<-py_to_r(record_data)












