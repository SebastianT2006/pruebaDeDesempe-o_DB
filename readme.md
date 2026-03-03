# DataBases

## install dependencies
npm install express multer csv-parse pg dotenv

## structure
    PRUEBADESEMPEÑO
    |
    |_docs
        |
        |_category.csv
        |_city.csv
        |_customer.csv
        |_sku.csv
        |_suppliers.csv
        |_transiction.csv
    _img
        |
        |_Imagen pegada.png
    |
    |_server.js
    |_README.md
## Tecnologies
### Docker
### Postman
### PostgresSQL

## normalization
I implemented this normalization because the city, category, sku, and suppliers tables are strong, and the others depend on them. The weakest table is transition, which depends on the strong tables, which in turn are weak in relation to the aforementioned tables.

## Searchs
### first
SELECT 
    s.id AS supplier_id,
    s.name AS supplier_name,
    SUM(t.quantity) AS total_items_vendidos,
    SUM(t.total_line_value) AS valor_total_generado
FROM transaction t
JOIN suppliers s ON t.supplier_id = s.id
GROUP BY s.id, s.name
ORDER BY total_items_vendidos DESC;

### second
SELECT 
    t.transaction_id,
    t.date,
    p.name AS product_name,
    t.quantity,
    t.total_line_value
FROM transaction t
JOIN product p ON t.product_id = p.id
WHERE t.customer_id = 'ID_DEL_CLIENTE'
ORDER BY t.date DESC;

### third
SELECT 
    p.id AS product_id,
    p.name AS product_name,
    SUM(t.quantity) AS total_vendido,
    SUM(t.total_line_value) AS ingresos_generados
FROM transaction t
JOIN product p ON t.product_id = p.id
JOIN category c ON p.category_id = c.id
WHERE c.id = 'ID_CATEGORIA'
GROUP BY p.id, p.name
ORDER BY ingresos_generados DESC;

## docker
ocker run --name postgres -p5434:5432 -e POSTGRES_PASSWORD=password -d postgres

## example in postman
http://localhost:3000/api/upload/transaction