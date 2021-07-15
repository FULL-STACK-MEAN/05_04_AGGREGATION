// Aggregation en MongoDB
// Pipe de etapas de transformación de datos que se ejecuta en memoria

// Sintaxis
// db.<coleccion>.aggregate(
// [
//      {etapa1} // empleo de operadores de aggregation
//      {etapa2},
//      ...    
//]
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

db.clientes([
    {nombre: 'Juan', apellidos: 'Pérez', alta: new Date(2021, 4, 5), actividades: ['padel','tenis','esgrima']},
    {nombre: 'Luisa', apellidos: 'López', alta: new Date(2021, 5, 15), actividades: ['aquagym','tenis','step']},
    {nombre: 'Carlos', apellidos: 'Pérez', alta: new Date(2021, 6, 8), actividades: ['aquagym','padel','cardio']},
    {nombre: 'Sara', apellidos: 'Gómez', alta: new Date(2021, 4, 25), actividades: ['pesas','cardio','step']},
])