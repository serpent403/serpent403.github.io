// Execute the below in the "mongo" shell

// Selecting the database
use tutorial
 
// Input data
db.orders.insert({ cust_id: "A123", amount: 500, status: "A" })
db.orders.insert({ cust_id: "A123", amount: 250, status: "A" })
db.orders.insert({ cust_id: "B212", amount: 200, status: "A" })
db.orders.insert({ cust_id: "A123", amount: 300, status: "D" })
 
 
/*
* Task:
* Calculate the sum of all “amount”s for every “cust_id”
*/


// map function
var map = function() {
  emit(this.cust_id, this.amount);
}
 

// reduce function
var reduce = function(keyCustId, values) {
  return Array.sum(values);
}

// options for the map reduce query
var options = {
  query: { status: "A" },
  out: "order_totals"
}
 
db.orders.mapReduce(map, reduce, options)
 
/*
* Output:
* > db.order_totals.find()
* { "_id" : "A123", "value" : 750 }
* { "_id" : "B212", "value" : 200 }
*/
