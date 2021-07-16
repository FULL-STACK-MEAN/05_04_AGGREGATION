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
