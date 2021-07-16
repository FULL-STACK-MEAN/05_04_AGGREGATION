// Aggregation en MongoDB
// Pipe de etapas de transformación de datos que se ejecuta en memoria

// Sintaxis
// db.<coleccion>.aggregate(
// [
//      {etapa1} // empleo de operadores de aggregation
//      {etapa2},
//      ...    
//], {allowDiskUse: true} / Elimina el límite de uso de memoria de 100 MB por etapa
//)

// Tendremos un resultado final al concluir el pipe que se puede o bien devolver en
// la operación que solicita la agregación o bien se puede escribir en una colección
// diferente

// Mantiene inmutable la colección aunque en las etapas se puedan ejecutar transformaciones
// de datos

// Etapa
// {$operadorEtapa: {$operador, $operador, ...}}

use biblioteca

// Operador $project (etapa)

db.libros.aggregate(
    [
        {$project: {titulo: 1, autor: 1, _id: 0}}
    ]
)

// Concepto de etapa, cada etapa devuelve un set de documentos que
// son usados en la entrada de la siguiente etapa
// field-reference "$<nombre-campo-etapa-anterior>", esta sintaxis sirve para hacer referencia a campos
// de la etapa anterior

db.libros.aggregate(
    [
        {$project: {titulo: 1, autor: 1, _id: 0}},
        {$project: {title: "$titulo", author: "$autor"}}
    ]
)

// Operador $sort (etapa)

use gimnasio2

db.clientes.insert([
    {nombre: 'Juan', apellidos: 'Pérez', alta: new Date(2021, 4, 5), actividades: ['padel','tenis','esgrima']},
    {nombre: 'Luisa', apellidos: 'López', alta: new Date(2021, 5, 15), actividades: ['aquagym','tenis','step']},
    {nombre: 'Carlos', apellidos: 'Pérez', alta: new Date(2021, 6, 8), actividades: ['aquagym','padel','cardio']},
    {nombre: 'Sara', apellidos: 'Gómez', alta: new Date(2021, 4, 25), actividades: ['pesas','cardio','step']},
])

db.clientes.aggregate(
    [
        {$project: {cliente: {$toUpper: "$apellidos"}, _id: 0}},
        {$sort: {cliente: 1}}
    ]
)

{ "cliente" : "GóMEZ" }
{ "cliente" : "LóPEZ" }
{ "cliente" : "PéREZ" }
{ "cliente" : "PéREZ" }

db.clientes.aggregate(
    [
        {$project: {nombre: 1, apellidos: 1, _id: 0, mesAlta: {$month: "$alta"}}},
        {$sort: {mesAlta: -1, apellidos: 1, nombre: 1}}
    ]    
)
{ "nombre" : "Carlos", "apellidos" : "Pérez", "mesAlta" : 7 }
{ "nombre" : "Luisa", "apellidos" : "López", "mesAlta" : 6 }
{ "nombre" : "Sara", "apellidos" : "Gómez", "mesAlta" : 5 }
{ "nombre" : "Juan", "apellidos" : "Pérez", "mesAlta" : 5 }

// $group 

// { $group: {
//     _id: <expresión>, agrupar por los resultados de la expresión
//     <campo>: {<acumulador>: <expresión},
//     <campo>...
//}}

db.clientes.aggregate(
    [
        {$project: {mesAlta: {$month: "$alta"}, _id: 0}},
        {$group: {_id: "$mesAlta", numeroAltasMes: {$sum: 1}}},
        {$project: {mes: "$_id", numeroAltasMes: 1, _id: 0}},
        {$sort: {numeroAltasMes: -1}}
    ], {allowDiskUse: true}
)
{ "numeroAltasMes" : 2, "mes" : 5 }
{ "numeroAltasMes" : 1, "mes" : 6 }
{ "numeroAltasMes" : 1, "mes" : 7 }

// Otros ejemplos de operadores

use shop3

db.pedidos.insert([
    {sku: 'V101', cantidad: 12, precio: 20, fecha: ISODate("2021-06-22")},
    {sku: 'V101', cantidad: 6, precio: 20, fecha: ISODate("2021-06-23")},
    {sku: 'V101', cantidad: 4, precio: 20, fecha: ISODate("2021-06-22")},
    {sku: 'V102', cantidad: 7, precio: 10.3, fecha: ISODate("2021-06-21")},
    {sku: 'V102', cantidad: 5, precio: 10.9, fecha: ISODate("2021-06-21")},
])

// Total ventas por dia de la semana

db.pedidos.aggregate([
    {$group: {_id: {$dayOfWeek: "$fecha"}, totalVentas: {$sum: {$multiply: ["$cantidad","$precio"]}}}},
    {$project: {diaSemana: "$_id", totalVentas: 1, _id: 0}},
    {$sort: {diaSemana: 1}}
])
{ "totalVentas" : 126.60000000000001, "diaSemana" : 2 }
{ "totalVentas" : 320, "diaSemana" : 3 }
{ "totalVentas" : 120, "diaSemana" : 4 }

// Promedio de cantidad de producto en cada pedido

db.pedidos.aggregate([
    {$group: {_id: "$sku", cantidadPromedio: {$avg: "$cantidad"}}},
    {$project: {skuProducto: "$_id", cantidadPromedio: 1, _id: 0}}
])
{ "cantidadPromedio" : 6, "skuProducto" : "V102" }
{ "cantidadPromedio" : 7.333333333333333, "skuProducto" : "V101" }

// Para crear arrays, por ejemplo un array de libros para cada autor de la colección

use biblioteca

db.libros.aggregate([
    {$group: {_id: "$autor", libros: {$push: "$titulo"}}},
    {$project: {autor: "$_id", libros: 1, _id: 0}}
])

// $group se puede usar con varios campos como agrupadores

use marathon  // Los corredores que tienen el mismo nombre y la misma edad

db.runners.aggregate([
    {$group: {_id: {nombre: "$name", edad: "$age"}, totalMismoNombreMismaEdad: {$sum: 1}}},
    {$project: {nombre: "$_id.nombre", edad: "$_id.edad", totalMismoNombreMismaEdad: 1, _id: 0}},
    {$sort: {nombre: 1, edad: 1}}
])

// $unwind Deconstruye un array en sus elementos

use shop2

db.items.insert([
    {nombre: "Camiseta", marca: "Nike", tallas: ["xs","s","m","l","xl"]},
    {nombre: "Camiseta", marca: "Puma", tallas: null},
    {nombre: "Camiseta", marca: "Adidas"},
])

// Crea un nuevo documento con todos los datos y uno de los elementos del array

db.items.aggregate([
    {$unwind: "$tallas"},
    {$project: {nombre: 1, marca: 1, talla: "$tallas", _id: 0}}
])
{ "nombre" : "Camiseta", "marca" : "Nike", "talla" : "xs" }
{ "nombre" : "Camiseta", "marca" : "Nike", "talla" : "s" }
{ "nombre" : "Camiseta", "marca" : "Nike", "talla" : "m" }
{ "nombre" : "Camiseta", "marca" : "Nike", "talla" : "l" }
{ "nombre" : "Camiseta", "marca" : "Nike", "talla" : "xl" }

// Opciones de $unwind

db.items.aggregate([
    {$unwind: {path: "$tallas", includeArrayIndex: "posicion"}}, // devuelve el index en el campo posicion
    {$project: {nombre: 1, marca: 1, talla: "$tallas", posicion: 1, _id: 0}}
])
{ "nombre" : "Camiseta", "marca" : "Nike", "posicion" : NumberLong(0), "talla" : "xs" }
{ "nombre" : "Camiseta", "marca" : "Nike", "posicion" : NumberLong(1), "talla" : "s" }
{ "nombre" : "Camiseta", "marca" : "Nike", "posicion" : NumberLong(2), "talla" : "m" }
{ "nombre" : "Camiseta", "marca" : "Nike", "posicion" : NumberLong(3), "talla" : "l" }
{ "nombre" : "Camiseta", "marca" : "Nike", "posicion" : NumberLong(4), "talla" : "xl" }


db.items.aggregate([
    {$unwind: {path: "$tallas", preserveNullAndEmptyArrays: true}}, // incluye los que tengan el array null o no lo tengan
    {$project: {nombre: 1, marca: 1, talla: "$tallas", _id: 0}}
])
{ "nombre" : "Camiseta", "marca" : "Nike", "talla" : "xs" }
{ "nombre" : "Camiseta", "marca" : "Nike", "talla" : "s" }
{ "nombre" : "Camiseta", "marca" : "Nike", "talla" : "m" }
{ "nombre" : "Camiseta", "marca" : "Nike", "talla" : "l" }
{ "nombre" : "Camiseta", "marca" : "Nike", "talla" : "xl" }
{ "nombre" : "Camiseta", "marca" : "Puma", "talla" : null }
{ "nombre" : "Camiseta", "marca" : "Adidas" }


// Ejemplo pregunta certificación
db.items.insert([
    {nombre: "Camiseta", marca: "Nike", tallas: ["xs","s","m","l","xl"]},
    {nombre: "Camiseta", marca: "New Balance", tallas: ["xs","s"]},
    {nombre: "Camiseta", marca: "Puma", tallas: null},
    {nombre: "Camiseta", marca: "Adidas"},
])

// ¿Qué número de documentos devuelve la operación?

db.items.aggregate([
    {$unwind: {path: "$tallas", preserveNullAndEmptyArrays: true}}, 
    {$project: {nombre: 1, marca: 1, talla: "$tallas", _id: 0}}
])

// a) 0
// b) 4
// c) 5
// d) 7
// e) 9 ok!

use gimnasio2  // Actividades favoritas de los clientes

db.clientes.aggregate([
    {$unwind: "$actividades"},
    {$group: {_id: "$actividades", totalClientes: {$sum: 1}}},
    {$project: {actividad: "$_id", totalClientes: 1, _id: 0}},
    {$sort: {totalClientes: -1}}
])

// $match 
// Utiliza la misma sintaxis de doc de consulta en find() findOne() update() etc...

use marathon

db.runners.aggregate([
    {$match: {$and: [{age: {$gte: 40}}, {age: {$lt: 50}}]}},
    {$group: {_id: "$age", total: {$sum: 1}}},
    {$project: {edad: "$_id", total: 1, _id: 0}},
    {$sort: {edad: 1}}
])


// Ejemplo con índices de texto

use shop4

db.opiniones.insert([
    {nombre: 'Nike Revolution', user: "00012", opinion: "buen servicio pero producto en mal estado"},
    {nombre: 'Nike Revolution', user: "00013", opinion: "muy satisfecho con la compra"},
    {nombre: 'Nike Revolution', user: "00014", opinion: "muy mal, tuve que devolverlas"},
    {nombre: 'Adidas Peace', user: "00014", opinion: "perfecto en todos los sentidos"},
    {nombre: 'Adidas Peace', user: "00013", opinion: "muy bien, muy contento"},
    {nombre: 'Adidas Peace', user: "00012", opinion: "mal, no me han gustado"},
    {nombre: 'Nike Revolution', user: "00015", opinion: "mal, no volveré a comprar"},
])

db.opiniones.createIndex({opinion: "text"})

db.opiniones.aggregate([
    {$match: {$text: {$search: "mal"}}},
    {$group: {_id: "$nombre", malasOpiniones: {$sum: 1}}},
    {$project: {producto: "$_id", malasOpiniones: 1, _id: 0}},
    {$sort: {malasOpiniones: -1}}
])

{ "malasOpiniones" : 3, "producto" : "Nike Revolution" }
{ "malasOpiniones" : 1, "producto" : "Adidas Peace" }

// $addFields

use marathon

db.results.insert([
    {name: 'Juan', arrive: new Date("2021-07-16T16:31:40")},
    {name: 'Laura', arrive: new Date("2021-07-16T15:43:27")},
    {name: 'Lucía', arrive: new Date("2021-07-16T17:10:22")},
    {name: 'Carlos', arrive: new Date("2021-07-16T16:15:09")},
])

db.results.aggregate([
    {$addFields: {time: {$subtract: ["$arrive", new Date("2021-07-16T12:00:00")]}}},
    {$addFields: {hours: {$floor: {$mod: [{$divide: ["$time", 60 * 60 * 1000]}, 24]}}}},
    {$addFields: {minutes: {$floor: {$mod: [{$divide: ["$time", 60 * 1000]}, 60]}}}},
    {$addFields: {seconds: {$mod: [{$divide: ["$time", 1000]}, 60]}}},
    {$sort: {time: 1}},
    {$project: {name: 1, hours: 1, minutes: 1, seconds: 1, _id: 0}}
])

// $skip <entero>

// $limit <entero>

// $merge (salida vaya a otra colección de Mongo)

use marathon

db.runners.aggregate([
    {$match: {$and: [{age: {$gte: 50}}, {age: {$lt: 60}}]}},
    {$group: {_id: "$age", total: {$sum: 1}}},
    {$project: {edad: "$_id", total: 1, _id: 0}},
    {$sort: {edad: 1}},
    {$merge: "resumen"} // la colección donde se insertarán el set de datos que produce la agregación
])

// Para enviar a una colección de otra base de datos y controlar la actualización de documentos

db.runners.aggregate([
    {$match: {$and: [{age: {$gte: 50}}, {age: {$lt: 60}}]}},
    {$group: {_id: "$age", total: {$sum: 1}}},
    {$project: {edad: "$_id", total: 1, _id: 0}},
    {$sort: {edad: 1}},
    {$merge: {
        into: {db: "marathonMadrid", coll: "resumenMadrid"},
        on: "dni",
        whenMatched: "keepExisting",
        whenNotMatched: "insert"
    }}
])

// $lookup es realizar un determinado tipo de join en MongoDB (left outer)
// Sintaxis
// { $lookup: {
//      from: <colección-externa>,
//      localField: <campo-de-la-coleccion>
//      forignField: <campo-de-la-colección-externa>
//      as: <nombre del campo de salida (array)>
// }}

use shop5

db.pedidos.insert([
    {_id: 1, items: [
        {codigo: "a01", precio: 12, cantidad: 2},
        {codigo: "j02", precio: 10, cantidad: 4},
    ]},
    {_id: 2, items: [
        {codigo: "j01", precio: 20, cantidad: 1},
    ]},
    {_id: 3, items: [
        {codigo: "j01", precio: 20, cantidad: 4},
    ]},
])

db.productos.insert([
    {_id: 1, codigo: "a01", descripcion: "producto 1", stock: 120},
    {_id: 2, codigo: "d01", descripcion: "producto 2", stock: 80},
    {_id: 3, codigo: "j01", descripcion: "producto 3", stock: 60},
    {_id: 4, codigo: "j02", descripcion: "producto 4", stock: 70}
])

// Operación de agregación desde la colección pedidos para obtener los datos
// de cada producto comprado (desde la otra colección productos)

db.pedidos.aggregate([
    {$unwind: "$items"},
    {$lookup: {
        from: "productos",
        localField: "items.codigo",
        foreignField: "codigo",
        as: "producto"
    }},
    {$unwind: "$producto"},
    {$project: {
        numeroPedido: "$_id", 
        producto: "$items.codigo",
        descripcion: "$producto.descripcion",
        stock: "$producto.stock",
        cantidad: "$items.cantidad",
        precio: "$items.precio",
        _id: 0
    }}
])
{ "numeroPedido" : 1, "producto" : "a01", "descripcion" : "producto 1", "stock" : 120, "cantidad" : 2, "precio" : 12 }
{ "numeroPedido" : 1, "producto" : "j02", "descripcion" : "producto 4", "stock" : 70, "cantidad" : 4, "precio" : 10 }
{ "numeroPedido" : 2, "producto" : "j01", "descripcion" : "producto 3", "stock" : 60, "cantidad" : 1, "precio" : 20 }
{ "numeroPedido" : 3, "producto" : "j01", "descripcion" : "producto 3", "stock" : 60, "cantidad" : 4, "precio" : 20 }

