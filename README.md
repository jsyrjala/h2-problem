# h2 1.4.198 regression

`App.java` contains demostration about locking/transction issue in h2 1.4.198.

1. Inserts 100 rows to the table.
2. In two threads:
   - query 75 rows
   - try reserving queried rows by writing thread id to row if it has not yet been written
   - check the result of the update to see if the reservation was successful 
   
The idea is ensure that each row gets reserved maximum by 1 thread.

With 1.4.198 one of the rows gets reserved by both threads. 
In 1.4.197 this worked correctly: no row gets reserved by two threads. 

You can change the 