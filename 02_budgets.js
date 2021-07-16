let customers = [
    { "name" : "Gas Natural", "cif" : "A12345668", "adress" : "Gran Vía 40", "cp" : "28001", "city" : "Madrid", "contact" : { "name" : "Juan", "surname" : "Pérez", "phone" : "666543132", "email" : "juan@gas.com" }, "createdAt" : ISODate("2021-06-21T15:26:03.180Z"), "updatedAt" : ISODate("2021-06-21T15:26:03.180Z"), "metaName" : "Gas Natural" },
    { "name" : "Jazztel España S.A.", "cif" : "A12345654", "adress" : "Príncipe de Vergara 12", "cp" : "28010", "city" : "Madrid", "contact" : { "name" : "Laura", "surname" : "Gómez", "phone" : "", "email" : "laura@jazztel.com" }, "createdAt" : ISODate("2021-06-21T16:37:10.376Z"), "updatedAt" : ISODate("2021-06-22T15:04:43.175Z"), "metaName" : "Jazztel España S.A." },
    { "name" : "Telefónica España S.A.", "cif" : "A44412342", "adress" : "Gran Vía 30", "cp" : "28001", "city" : "Madrid", "contact" : { "name" : "Juan", "surname" : "López", "phone" : "", "email" : "juan@telefonica.com" }, "createdAt" : ISODate("2021-06-21T17:18:28.944Z"), "updatedAt" : ISODate("2021-06-22T14:56:06.816Z"), "metaName" : "Telefonica España S.A." },
    { "name" : "Orange", "cif" : "A44433311", "adress" : "Serrano Galvache 30", "cp" : "28033", "city" : "Madrid", "contact" : { "name" : "Lucía", "surname" : "Gómez", "phone" : "", "email" : "lgomez@orange.com" }, "createdAt" : ISODate("2021-06-22T16:14:28.755Z"), "updatedAt" : ISODate("2021-06-22T16:14:28.755Z"), "metaName" : "Orange" },
    { "name" : "Álvarez Distribuciones, S.L.", "metaName" : "alvarez Distribuciones, S.L.", "cif" : "B12334444", "adress" : "Lorem", "cp" : "28001", "city" : "Madrid", "contact" : { "name" : "Juan", "surname" : "Álvarez", "phone" : "", "email" : "a@a.com" }, "createdAt" : ISODate("2021-07-14T14:48:51.183Z"), "updatedAt" : ISODate("2021-07-14T14:48:51.183Z") },
]

let budgets = [
    { "customer" : { "contact" : { "name" : "Juan", "surname" : "Pérez", "phone" : "666543132", "email" : "juan@gas.com" }, "_id" : "60eddd49e52e6eed520450f0", "name" : "Gas Natural", "cif" : "A12345668", "adress" : "Gran Vía 40", "cp" : "28001", "city" : "Madrid", "createdAt" : "2021-06-21T15:26:03.180Z", "updatedAt" : "2021-06-21T15:26:03.180Z", "metaName" : "Gas Natural" }, "code" : "001-2021", "date" : ISODate("2021-05-19T00:00:00Z"), "validUntil" : ISODate("2021-08-15T00:00:00Z"), "items" : [ { "article" : "Lorem1", "quantity" : 10, "price" : 20, "amount" : 200 }, { "article" : "Lorem2", "quantity" : 20, "price" : 100, "amount" : 2000 } ], "idSalesUser" : "60edde18b4af3401007c8ca1", "createdAt" : ISODate("2021-07-16T16:56:49.248Z"), "updatedAt" : ISODate("2021-07-16T16:56:49.248Z") },
    { "customer" : { "contact" : { "name" : "Laura", "surname" : "Gómez", "phone" : "", "email" : "laura@jazztel.com" }, "_id" : "60eddd49e52e6eed520450f1", "name" : "Jazztel España S.A.", "cif" : "A12345654", "adress" : "Príncipe de Vergara 12", "cp" : "28010", "city" : "Madrid", "createdAt" : "2021-06-21T16:37:10.376Z", "updatedAt" : "2021-06-22T15:04:43.175Z", "metaName" : "Jazztel España S.A." }, "code" : "002-2021", "date" : ISODate("2021-05-19T00:00:00Z"), "validUntil" : ISODate("2021-08-15T00:00:00Z"), "items" : [ { "article" : "Lorem2", "quantity" : 20, "price" : 140, "amount" : 2800 } ], "idSalesUser" : "60edde18b4af3401007c8ca1", "createdAt" : ISODate("2021-07-16T16:57:40.730Z"), "updatedAt" : ISODate("2021-07-16T16:57:40.730Z") },
    { "customer" : { "contact" : { "name" : "Juan", "surname" : "López", "phone" : "", "email" : "juan@telefonica.com" }, "_id" : "60eddd49e52e6eed520450f2", "name" : "Telefónica España S.A.", "cif" : "A44412342", "adress" : "Gran Vía 30", "cp" : "28001", "city" : "Madrid", "createdAt" : "2021-06-21T17:18:28.944Z", "updatedAt" : "2021-06-22T14:56:06.816Z", "metaName" : "Telefonica España S.A." }, "code" : "003-2021", "date" : ISODate("2021-06-15T00:00:00Z"), "validUntil" : ISODate("2021-08-15T00:00:00Z"), "items" : [ { "article" : "Lorem1", "quantity" : 10, "price" : 100, "amount" : 1000 }, { "article" : "Lorem2", "quantity" : 20, "price" : 60, "amount" : 1200 } ], "idSalesUser" : "60edde18b4af3401007c8ca1", "createdAt" : ISODate("2021-07-16T16:58:10.898Z"), "updatedAt" : ISODate("2021-07-16T16:58:10.898Z") },
    { "customer" : { "contact" : { "name" : "Juan", "surname" : "López", "phone" : "", "email" : "juan@telefonica.com" }, "_id" : "60eddd49e52e6eed520450f2", "name" : "Telefónica España S.A.", "cif" : "A44412342", "adress" : "Gran Vía 30", "cp" : "28001", "city" : "Madrid", "createdAt" : "2021-06-21T17:18:28.944Z", "updatedAt" : "2021-06-22T14:56:06.816Z", "metaName" : "Telefonica España S.A." }, "code" : "004-2021", "date" : ISODate("2021-06-30T00:00:00Z"), "validUntil" : ISODate("2021-08-15T00:00:00Z"), "items" : [ { "article" : "Lorem1", "quantity" : 100, "price" : 90, "amount" : 9000 } ], "idSalesUser" : "60edde18b4af3401007c8ca1", "createdAt" : ISODate("2021-07-16T16:58:37.052Z"), "updatedAt" : ISODate("2021-07-16T16:58:37.052Z") },
    { "customer" : { "contact" : { "name" : "Juan", "surname" : "Pérez", "phone" : "666543132", "email" : "juan@gas.com" }, "_id" : "60eddd49e52e6eed520450f0", "name" : "Gas Natural", "cif" : "A12345668", "adress" : "Gran Vía 40", "cp" : "28001", "city" : "Madrid", "createdAt" : "2021-06-21T15:26:03.180Z", "updatedAt" : "2021-06-21T15:26:03.180Z", "metaName" : "Gas Natural" }, "code" : "005-2021", "date" : ISODate("2021-07-16T00:00:00Z"), "validUntil" : ISODate("2021-08-15T00:00:00Z"), "items" : [ { "article" : "Lorem1", "quantity" : 10, "price" : 190, "amount" : 1900 } ], "idSalesUser" : "60edde18b4af3401007c8ca1", "createdAt" : ISODate("2021-07-16T16:59:00.488Z"), "updatedAt" : ISODate("2021-07-16T16:59:00.488Z") },
    { "customer" : { "contact" : { "name" : "Laura", "surname" : "Gómez", "phone" : "", "email" : "laura@jazztel.com" }, "_id" : "60eddd49e52e6eed520450f1", "name" : "Jazztel España S.A.", "cif" : "A12345654", "adress" : "Príncipe de Vergara 12", "cp" : "28010", "city" : "Madrid", "createdAt" : "2021-06-21T16:37:10.376Z", "updatedAt" : "2021-06-22T15:04:43.175Z", "metaName" : "Jazztel España S.A." }, "code" : "006-2021", "date" : ISODate("2021-07-16T00:00:00Z"), "validUntil" : ISODate("2021-08-15T00:00:00Z"), "items" : [ { "article" : "Lorem", "quantity" : 50, "price" : 230, "amount" : 11500 } ], "idSalesUser" : "60edde18b4af3401007c8ca1", "createdAt" : ISODate("2021-07-16T16:59:19.491Z"), "updatedAt" : ISODate("2021-07-16T16:59:19.491Z") },
    { "customer" : { "contact" : { "name" : "Juan", "surname" : "Álvarez", "phone" : "", "email" : "a@a.com" }, "_id" : "60eef9534660d4190c02afb9", "name" : "Álvarez Distribuciones, S.L.", "metaName" : "alvarez Distribuciones, S.L.", "cif" : "B12334444", "adress" : "Lorem", "cp" : "28001", "city" : "Madrid", "createdAt" : "2021-07-14T14:48:51.183Z", "updatedAt" : "2021-07-14T14:48:51.183Z" }, "code" : "007-2021", "date" : ISODate("2021-07-16T00:00:00Z"), "validUntil" : ISODate("2021-08-15T00:00:00Z"), "items" : [ { "article" : "Lorem1", "quantity" : 100, "price" : 201, "amount" : 20100 } ], "idSalesUser" : "60edde18b4af3401007c8ca1", "createdAt" : ISODate("2021-07-16T16:59:41.705Z"), "updatedAt" : ISODate("2021-07-16T16:59:41.705Z") },
]

// La primera operación de agregación necesita devolver las ventas acumuladas por cliente (de los
// presupuestos)

// customers
// totals

db.budgets.aggregate([
    {$unwind: "$items"},
    {$group: {_id: "$customer.name", totals: {$sum: "$items.amount"}}},
    {$project: {customer: "$_id", totals: 1, _id: 0}},
    {$sort: {totals: -1}}
])

// La segunda operación de agregación necesita devolver las ventas acumuladas por mes (3 últimos meses)

// months
// totals

db.budgets.aggregate([
    {$unwind: "$items"},
    {$group: {_id: {month: {$month: "$date"}, year: {$year: "$date"}}, totals: {$sum: "$items.amount"}}},
    {$project: {month: "$_id.month", year: "$_id.year", totals: 1, _id: 0}},
    {$sort: {year: -1, month: -1}},
    {$limit: 3}
])