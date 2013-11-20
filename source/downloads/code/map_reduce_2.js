// Execute the below in the "mongo" shell

// Selecting the database
use tutorial

// Input data
db.carts.insert({ cust_id: "A123", amount: 25, status: "A", items: [ { sku: "mmm", qty: 5, price: 2.5 }, { sku: "nnn", qty: 5, price: 2.5 } ] })
db.carts.insert({ cust_id: "A123", amount: 50, status: "A", items: [ { sku: "mmm", qty: 10, price: 5 } ] })
db.carts.insert({ cust_id: "B212", amount: 30, status: "A", items: [ { sku: "nnn", qty: 2, price: 5 }, { sku: "kkk", qty: 1, price: 20 } ] })
db.carts.insert({ cust_id: "A123", amount: 90, status: "D", items: [ { sku: "mmm", qty: 10, price: 6 }, { sku: "kkk", qty: 3, price: 10 } ] })


/*
* Task:
* Calculate the total count, total quantity and average quantity for every “sku”
*/

// map function
var map = function() {
  for(var idx = 0; idx < this.items.length; idx++) {
    var key = this.items[idx].sku;
    var value = { count: 1, qty: this.items[idx].qty };
    emit(key, value);
  }
}
 
// reduce function
var reduce = function(keySku, values) {
  reducedVal = { count: 0, qty: 0 };
 
  for(var idx = 0; idx < values.length; idx++){
    reducedVal.count += values[idx].count;
    reducedVal.qty += values[idx].qty;
  }
 
  return reducedVal;
}

// finalize function
var finalize = function(keySku, reducedVal) {
  reducedVal.avg = reducedVal.qty / reducedVal.count;
  return reducedVal;
}

// options for the map reduce query
var options = {
  out: { merge: "cart_sku_results" },
  finalize: finalize
}
 
db.carts.mapReduce(map, reduce, options)
 
 
/*
* Output:
* { "_id" : "kkk", "value" : { "count" : 2, "qty" : 4, "avg" : 2 } }
* { "_id" : "mmm", "value" : { "count" : 3, "qty" : 25, "avg" : 8.333333333333334 } }
* { "_id" : "nnn", "value" : { "count" : 2, "qty" : 7, "avg" : 3.5 } }
*/
