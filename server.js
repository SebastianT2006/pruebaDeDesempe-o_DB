// ==================== IMPORTAR LIBRERÍAS ====================
const express = require('express');
const multer = require('multer');
const { parse } = require('csv-parse');
const { Pool } = require('pg');
const fs = require('fs');
const path = require('path');

// ==================== CREAR SERVIDOR ====================
const app = express();
app.use(express.json());

// Crear carpeta uploads si no existe
const uploadDir = 'uploads';
if (!fs.existsSync(uploadDir)) {
    fs.mkdirSync(uploadDir, { recursive: true });
}

const upload = multer({ dest: uploadDir });

// ==================== CONEXIÓN POSTGRESQL ====================
const pool = new Pool({
    user: 'postgres',
    host: 'localhost',
    database: 'postgres',
    password: 'password',
    port: 5434,
});

// ==================== FUNCIÓN PARA GUARDAR LOGS ====================
async function saveLog(action, table, count) {
    console.log(`[LOG] ${action} - Tabla: ${table} - Registros: ${count}`);
}

// ==================== Nombre tablas ====================
const TABLA1_nombre = 'category';      
const TABLA2_nombre = 'city';  
const TABLA3_nombre = 'sku';     
const TABLA4_nombre = 'customer';      
const TABLA5_nombre = 'product';      
const TABLA6_nombre = 'suppliers';    
const TABLA7_nombre = 'transaction';  

// ==================== CREAR TABLAS ====================
async function createTables() {
    try {
        // TABLA 1: category
        await pool.query(`
            CREATE TABLE IF NOT EXISTS ${TABLA1_nombre} (
                id VARCHAR(50) PRIMARY KEY,
                name VARCHAR(255) NOT NULL UNIQUE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        `);
        console.log(`✓ ${TABLA1_nombre}`);

        // TABLA 2: city
        await pool.query(`
            CREATE TABLE IF NOT EXISTS ${TABLA2_nombre} (
                id VARCHAR(50) PRIMARY KEY,
                name VARCHAR(255) NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        `);
        console.log(`✓ ${TABLA2_nombre}`);

        // TABLA 3: sku
        await pool.query(`
            CREATE TABLE IF NOT EXISTS ${TABLA3_nombre} (
                id VARCHAR(50) PRIMARY KEY,
                name VARCHAR(255) NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        `);
        console.log(`✓ ${TABLA3_nombre}`);

        // TABLA 4: customer
        await pool.query(`
            CREATE TABLE IF NOT EXISTS ${TABLA4_nombre} (
                id VARCHAR(50) PRIMARY KEY,
                name VARCHAR(255) NOT NULL,
                lastname VARCHAR(255) NOT NULL,
                email VARCHAR(255),
                address VARCHAR(255),
                city_id VARCHAR(50),
                phone VARCHAR(20),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (city_id) REFERENCES ${TABLA2_nombre}(id) ON DELETE CASCADE
            );
        `);
        console.log(`✓ ${TABLA4_nombre}`);

        // TABLA 5: product
        await pool.query(`
            CREATE TABLE IF NOT EXISTS ${TABLA5_nombre} (
                id VARCHAR(50) PRIMARY KEY,
                category_id VARCHAR(50) NOT NULL,
                sku_id VARCHAR(50) NOT NULL,
                name VARCHAR(255) NOT NULL,
                unit_price FLOAT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (category_id) REFERENCES ${TABLA1_nombre}(id) ON DELETE CASCADE,
                FOREIGN KEY (sku_id) REFERENCES ${TABLA3_nombre}(id) ON DELETE CASCADE
            );
        `);
        console.log(`✓ ${TABLA5_nombre}`);

        // TABLA 6: suppliers
        await pool.query(`
            CREATE TABLE IF NOT EXISTS ${TABLA6_nombre} (
                id VARCHAR(50) PRIMARY KEY,
                name VARCHAR(255) NOT NULL,
                email VARCHAR(255),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        `);
        console.log(`✓ ${TABLA6_nombre}`);

        // TABLA 7: transaction
        await pool.query(`
            CREATE TABLE IF NOT EXISTS ${TABLA7_nombre} (
                id VARCHAR(50) PRIMARY KEY,
                transaction_id VARCHAR(255) NOT NULL,
                date DATE,
                customer_id VARCHAR(50) NOT NULL,
                product_id VARCHAR(50) NOT NULL,
                quantity INT,
                total_line_value FLOAT,
                supplier_id VARCHAR(50),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (customer_id) REFERENCES ${TABLA4_nombre}(id) ON DELETE CASCADE,
                FOREIGN KEY (product_id) REFERENCES ${TABLA5_nombre}(id) ON DELETE CASCADE,
                FOREIGN KEY (supplier_id) REFERENCES ${TABLA6_nombre}(id) ON DELETE CASCADE
            );
        `);
        console.log(`✓ ${TABLA7_nombre}`);
    } catch (error) {
        console.error('❌ Error creando tablas:', error.message);
    }
}
createTables();

// ==================== ENDPOINT: SUBIR A TABLA1 (category) ====================
app.post(`/api/upload/${TABLA1_nombre}`, upload.single('archivo'), async (req, res) => {
    if (!req.file) {
        return res.status(400).json({ error: 'No file provided' });
    }

    const rows = [];
    
    try {
        fs.createReadStream(req.file.path)
            .pipe(parse({ columns: true, trim: true }))
            .on('data', (row) => rows.push(row))
            .on('error', (err) => {
                console.error('Parse error:', err);
                if (req.file && fs.existsSync(req.file.path)) {
                    fs.unlinkSync(req.file.path);
                }
                res.status(400).json({ error: 'Error parsing CSV' });
            })
            .on('end', async () => {
                try {
                    if (rows.length === 0) {
                        fs.unlinkSync(req.file.path);
                        return res.status(400).json({ error: 'CSV vacío' });
                    }
                    
                    let insertedCount = 0;
                    let errors = [];

                    for (const row of rows) {
                        try {
                            if (!row.id || !row.name) {
                                errors.push(`Fila sin ID o nombre: ${JSON.stringify(row)}`);
                                continue;
                            }

                            await pool.query(
                                `INSERT INTO ${TABLA1_nombre} (id, name) VALUES ($1, $2) ON CONFLICT (id) DO NOTHING`,
                                [row.id, row.name]
                            );
                            insertedCount++;
                        } catch (err) {
                            console.warn('Error insertando fila:', err.message);
                            errors.push(err.message);
                        }
                    }

                    await saveLog('INSERT', TABLA1_nombre, insertedCount);
                    fs.unlinkSync(req.file.path);
                    
                    res.json({ 
                        ok: true, 
                        total: rows.length, 
                        inserted: insertedCount,
                        errors: errors.length > 0 ? errors : null
                    });
                } catch (error) {
                    console.error('Error final:', error);
                    if (req.file && fs.existsSync(req.file.path)) {
                        fs.unlinkSync(req.file.path);
                    }
                    res.status(500).json({ error: 'Error procesando datos', details: error.message });
                }
            });
    } catch (error) {
        console.error('Error:', error);
        if (req.file && fs.existsSync(req.file.path)) {
            fs.unlinkSync(req.file.path);
        }
        res.status(500).json({ error: 'Error al procesar archivo', details: error.message });
    }
});

app.get(`/${TABLA1_nombre}`, async (req, res) => {
    try {
        const result = await pool.query(`SELECT * FROM ${TABLA1_nombre} ORDER BY created_at DESC`);
        res.json(result.rows);
    } catch (error) {
        res.status(500).json({ error: error.message });
    }
});

// ==================== ENDPOINT: SUBIR A TABLA2 (city) ====================
app.post(`/api/upload/${TABLA2_nombre}`, upload.single('archivo'), async (req, res) => {
    if (!req.file) {
        return res.status(400).json({ error: 'No file provided' });
    }

    const rows = [];
    
    try {
        fs.createReadStream(req.file.path)
            .pipe(parse({ columns: true, trim: true }))
            .on('data', (row) => rows.push(row))
            .on('error', (err) => {
                console.error('Parse error:', err);
                if (req.file && fs.existsSync(req.file.path)) {
                    fs.unlinkSync(req.file.path);
                }
                res.status(400).json({ error: 'Error parsing CSV' });
            })
            .on('end', async () => {
                try {
                    if (rows.length === 0) {
                        fs.unlinkSync(req.file.path);
                        return res.status(400).json({ error: 'CSV vacío' });
                    }
                    
                    let insertedCount = 0;
                    let errors = [];

                    for (const row of rows) {
                        try {
                            if (!row.id || !row.name) {
                                errors.push(`Fila sin datos requeridos: ${JSON.stringify(row)}`);
                                continue;
                            }
                            
                            await pool.query(
                                `INSERT INTO ${TABLA2_nombre} (id, name) VALUES ($1, $2) ON CONFLICT (id) DO NOTHING`,
                                [row.id, row.name]
                            );
                            insertedCount++;
                        } catch (err) {
                            console.warn('Error insertando fila:', err.message);
                            errors.push(err.message);
                        }
                    }

                    await saveLog('INSERT', TABLA2_nombre, insertedCount);
                    fs.unlinkSync(req.file.path);
                    
                    res.json({ 
                        ok: true, 
                        total: rows.length, 
                        inserted: insertedCount,
                        errors: errors.length > 0 ? errors : null
                    });
                } catch (error) {
                    console.error('Error final:', error);
                    if (req.file && fs.existsSync(req.file.path)) {
                        fs.unlinkSync(req.file.path);
                    }
                    res.status(500).json({ error: 'Error procesando datos', details: error.message });
                }
            });
    } catch (error) {
        console.error('Error:', error);
        if (req.file && fs.existsSync(req.file.path)) {
            fs.unlinkSync(req.file.path);
        }
        res.status(500).json({ error: 'Error al procesar archivo', details: error.message });
    }
});

app.get(`/${TABLA2_nombre}`, async (req, res) => {
    try {
        const result = await pool.query(`SELECT * FROM ${TABLA2_nombre} ORDER BY created_at DESC`);
        res.json(result.rows);
    } catch (error) {
        res.status(500).json({ error: error.message });
    }
});

// ==================== ENDPOINT: SUBIR A TABLA3 (sku) ====================
app.post(`/api/upload/${TABLA3_nombre}`, upload.single('archivo'), async (req, res) => {
    if (!req.file) {
        return res.status(400).json({ error: 'No file provided' });
    }

    const rows = [];
    
    try {
        fs.createReadStream(req.file.path)
            .pipe(parse({ columns: true, trim: true }))
            .on('data', (row) => rows.push(row))
            .on('error', (err) => {
                console.error('Parse error:', err);
                if (req.file && fs.existsSync(req.file.path)) {
                    fs.unlinkSync(req.file.path);
                }
                res.status(400).json({ error: 'Error parsing CSV' });
            })
            .on('end', async () => {
                try {
                    if (rows.length === 0) {
                        fs.unlinkSync(req.file.path);
                        return res.status(400).json({ error: 'CSV vacío' });
                    }
                    
                    let insertedCount = 0;
                    let errors = [];

                    for (const row of rows) {
                        try {
                            if (!row.id || !row.name) {
                                errors.push(`Fila sin datos requeridos: ${JSON.stringify(row)}`);
                                continue;
                            }
                            
                            await pool.query(
                                `INSERT INTO ${TABLA3_nombre} (id, name) VALUES ($1, $2) ON CONFLICT (id) DO NOTHING`,
                                [row.id, row.name]
                            );
                            insertedCount++;
                        } catch (err) {
                            console.warn('Error insertando fila:', err.message);
                            errors.push(err.message);
                        }
                    }

                    await saveLog('INSERT', TABLA3_nombre, insertedCount);
                    fs.unlinkSync(req.file.path);
                    
                    res.json({ 
                        ok: true, 
                        total: rows.length, 
                        inserted: insertedCount,
                        errors: errors.length > 0 ? errors : null
                    });
                } catch (error) {
                    console.error('Error final:', error);
                    if (req.file && fs.existsSync(req.file.path)) {
                        fs.unlinkSync(req.file.path);
                    }
                    res.status(500).json({ error: 'Error procesando datos', details: error.message });
                }
            });
    } catch (error) {
        console.error('Error:', error);
        if (req.file && fs.existsSync(req.file.path)) {
            fs.unlinkSync(req.file.path);
        }
        res.status(500).json({ error: 'Error al procesar archivo', details: error.message });
    }
});

app.get(`/${TABLA3_nombre}`, async (req, res) => {
    try {
        const result = await pool.query(`SELECT * FROM ${TABLA3_nombre} ORDER BY created_at DESC`);
        res.json(result.rows);
    } catch (error) {
        res.status(500).json({ error: error.message });
    }
});

// ==================== ENDPOINT: SUBIR A TABLA4 (customer) ====================
app.post(`/api/upload/${TABLA4_nombre}`, upload.single('archivo'), async (req, res) => {
    if (!req.file) {
        return res.status(400).json({ error: 'No file provided' });
    }

    const rows = [];
    
    try {
        fs.createReadStream(req.file.path)
            .pipe(parse({ columns: true, trim: true }))
            .on('data', (row) => rows.push(row))
            .on('error', (err) => {
                console.error('Parse error:', err);
                if (req.file && fs.existsSync(req.file.path)) {
                    fs.unlinkSync(req.file.path);
                }
                res.status(400).json({ error: 'Error parsing CSV' });
            })
            .on('end', async () => {
                try {
                    if (rows.length === 0) {
                        fs.unlinkSync(req.file.path);
                        return res.status(400).json({ error: 'CSV vacío' });
                    }
                    
                    let insertedCount = 0;
                    let skippedCount = 0;
                    let errors = [];

                    for (const row of rows) {
                        try {
                            if (!row.id || !row.name || !row.lastname) {
                                skippedCount++;
                                errors.push(`Fila sin datos requeridos: ${JSON.stringify(row)}`);
                                continue;
                            }

                            // Validar city_id si existe
                            if (row.city_id) {
                                const cityCheck = await pool.query(
                                    `SELECT id FROM ${TABLA2_nombre} WHERE id = $1`,
                                    [row.city_id]
                                );
                                if (cityCheck.rows.length === 0) {
                                    skippedCount++;
                                    errors.push(`city_id ${row.city_id} no existe`);
                                    continue;
                                }
                            }
                            
                            await pool.query(
                                `INSERT INTO ${TABLA4_nombre} (id, name, lastname, email, address, city_id, phone) 
                                VALUES ($1, $2, $3, $4, $5, $6, $7) ON CONFLICT (id) DO NOTHING`,
                                [row.id, row.name, row.lastname, row.email || null, row.address || null, row.city_id || null, row.phone || null]
                            );
                            insertedCount++;
                        } catch (err) {
                            console.warn('Error insertando fila:', err.message);
                            skippedCount++;
                            errors.push(err.message);
                        }
                    }

                    await saveLog('INSERT', TABLA4_nombre, insertedCount);
                    fs.unlinkSync(req.file.path);
                    
                    res.json({ 
                        ok: true, 
                        total: rows.length, 
                        inserted: insertedCount,
                        skipped: skippedCount,
                        errors: errors.length > 0 ? errors : null
                    });
                } catch (error) {
                    console.error('Error final:', error);
                    if (req.file && fs.existsSync(req.file.path)) {
                        fs.unlinkSync(req.file.path);
                    }
                    res.status(500).json({ error: 'Error procesando datos', details: error.message });
                }
            });
    } catch (error) {
        console.error('Error:', error);
        if (req.file && fs.existsSync(req.file.path)) {
            fs.unlinkSync(req.file.path);
        }
        res.status(500).json({ error: 'Error al procesar archivo', details: error.message });
    }
});

app.get(`/${TABLA4_nombre}`, async (req, res) => {
    try {
        const result = await pool.query(`
            SELECT c.id, c.name, c.lastname, c.email, c.address, c.city_id, cy.name as city_name, c.phone, c.created_at
            FROM ${TABLA4_nombre} c
            LEFT JOIN ${TABLA2_nombre} cy ON c.city_id = cy.id
            ORDER BY c.created_at DESC
        `);
        res.json(result.rows);
    } catch (error) {
        res.status(500).json({ error: error.message });
    }
});

// ==================== ENDPOINT: SUBIR A TABLA5 (product) ====================
app.post(`/api/upload/${TABLA5_nombre}`, upload.single('archivo'), async (req, res) => {
    if (!req.file) {
        return res.status(400).json({ error: 'No file provided' });
    }

    const rows = [];
    
    try {
        fs.createReadStream(req.file.path)
            .pipe(parse({ columns: true, trim: true }))
            .on('data', (row) => rows.push(row))
            .on('error', (err) => {
                console.error('Parse error:', err);
                if (req.file && fs.existsSync(req.file.path)) {
                    fs.unlinkSync(req.file.path);
                }
                res.status(400).json({ error: 'Error parsing CSV' });
            })
            .on('end', async () => {
                try {
                    if (rows.length === 0) {
                        fs.unlinkSync(req.file.path);
                        return res.status(400).json({ error: 'CSV vacío' });
                    }
                    
                    let insertedCount = 0;
                    let skippedCount = 0;
                    let errors = [];

                    for (const row of rows) {
                        try {
                            if (!row.id || !row.category_id || !row.sku_id || !row.name) {
                                skippedCount++;
                                errors.push(`Fila sin datos requeridos: ${JSON.stringify(row)}`);
                                continue;
                            }

                            // Validar category_id
                            const catCheck = await pool.query(
                                `SELECT id FROM ${TABLA1_nombre} WHERE id = $1`,
                                [row.category_id]
                            );
                            if (catCheck.rows.length === 0) {
                                skippedCount++;
                                errors.push(`category_id ${row.category_id} no existe`);
                                continue;
                            }

                            // Validar sku_id
                            const skuCheck = await pool.query(
                                `SELECT id FROM ${TABLA3_nombre} WHERE id = $1`,
                                [row.sku_id]
                            );
                            if (skuCheck.rows.length === 0) {
                                skippedCount++;
                                errors.push(`sku_id ${row.sku_id} no existe`);
                                continue;
                            }
                            
                            await pool.query(
                                `INSERT INTO ${TABLA5_nombre} (id, category_id, sku_id, name, unit_price) 
                                VALUES ($1, $2, $3, $4, $5) ON CONFLICT (id) DO NOTHING`,
                                [row.id, row.category_id, row.sku_id, row.name, row.unit_price || null]
                            );
                            insertedCount++;
                        } catch (err) {
                            console.warn('Error insertando fila:', err.message);
                            skippedCount++;
                            errors.push(err.message);
                        }
                    }

                    await saveLog('INSERT', TABLA5_nombre, insertedCount);
                    fs.unlinkSync(req.file.path);
                    
                    res.json({ 
                        ok: true, 
                        total: rows.length, 
                        inserted: insertedCount,
                        skipped: skippedCount,
                        errors: errors.length > 0 ? errors : null
                    });
                } catch (error) {
                    console.error('Error final:', error);
                    if (req.file && fs.existsSync(req.file.path)) {
                        fs.unlinkSync(req.file.path);
                    }
                    res.status(500).json({ error: 'Error procesando datos', details: error.message });
                }
            });
    } catch (error) {
        console.error('Error:', error);
        if (req.file && fs.existsSync(req.file.path)) {
            fs.unlinkSync(req.file.path);
        }
        res.status(500).json({ error: 'Error al procesar archivo', details: error.message });
    }
});

app.get(`/${TABLA5_nombre}`, async (req, res) => {
    try {
        const result = await pool.query(`
            SELECT p.id, p.category_id, cat.name as category_name, p.sku_id, s.name as sku_name, p.name, p.unit_price, p.created_at
            FROM ${TABLA5_nombre} p
            LEFT JOIN ${TABLA1_nombre} cat ON p.category_id = cat.id
            LEFT JOIN ${TABLA3_nombre} s ON p.sku_id = s.id
            ORDER BY p.created_at DESC
        `);
        res.json(result.rows);
    } catch (error) {
        res.status(500).json({ error: error.message });
    }
});

// ==================== ENDPOINT: SUBIR A TABLA6 (suppliers) ====================
app.post(`/api/upload/${TABLA6_nombre}`, upload.single('archivo'), async (req, res) => {
    if (!req.file) {
        return res.status(400).json({ error: 'No file provided' });
    }

    const rows = [];
    
    try {
        fs.createReadStream(req.file.path)
            .pipe(parse({ columns: true, trim: true }))
            .on('data', (row) => rows.push(row))
            .on('error', (err) => {
                console.error('Parse error:', err);
                if (req.file && fs.existsSync(req.file.path)) {
                    fs.unlinkSync(req.file.path);
                }
                res.status(400).json({ error: 'Error parsing CSV' });
            })
            .on('end', async () => {
                try {
                    if (rows.length === 0) {
                        fs.unlinkSync(req.file.path);
                        return res.status(400).json({ error: 'CSV vacío' });
                    }
                    
                    let insertedCount = 0;
                    let errors = [];

                    for (const row of rows) {
                        try {
                            if (!row.id || !row.name) {
                                errors.push(`Fila sin ID o nombre: ${JSON.stringify(row)}`);
                                continue;
                            }
                            
                            await pool.query(
                                `INSERT INTO ${TABLA6_nombre} (id, name, email) VALUES ($1, $2, $3) ON CONFLICT (id) DO NOTHING`,
                                [row.id, row.name, row.email || null]
                            );
                            insertedCount++;
                        } catch (err) {
                            console.warn('Error insertando fila:', err.message);
                            errors.push(err.message);
                        }
                    }

                    await saveLog('INSERT', TABLA6_nombre, insertedCount);
                    fs.unlinkSync(req.file.path);
                    
                    res.json({ 
                        ok: true, 
                        total: rows.length, 
                        inserted: insertedCount,
                        errors: errors.length > 0 ? errors : null
                    });
                } catch (error) {
                    console.error('Error final:', error);
                    if (req.file && fs.existsSync(req.file.path)) {
                        fs.unlinkSync(req.file.path);
                    }
                    res.status(500).json({ error: 'Error procesando datos', details: error.message });
                }
            });
    } catch (error) {
        console.error('Error:', error);
        if (req.file && fs.existsSync(req.file.path)) {
            fs.unlinkSync(req.file.path);
        }
        res.status(500).json({ error: 'Error al procesar archivo', details: error.message });
    }
});

app.get(`/${TABLA6_nombre}`, async (req, res) => {
    try {
        const result = await pool.query(`SELECT * FROM ${TABLA6_nombre} ORDER BY created_at DESC`);
        res.json(result.rows);
    } catch (error) {
        res.status(500).json({ error: error.message });
    }
});

// ==================== ENDPOINT: SUBIR A TABLA7 (transaction) ====================
app.post(`/api/upload/${TABLA7_nombre}`, upload.single('archivo'), async (req, res) => {
    if (!req.file) {
        return res.status(400).json({ error: 'No file provided' });
    }

    const rows = [];
    
    try {
        fs.createReadStream(req.file.path)
            .pipe(parse({ columns: true, trim: true }))
            .on('data', (row) => rows.push(row))
            .on('error', (err) => {
                console.error('Parse error:', err);
                if (req.file && fs.existsSync(req.file.path)) {
                    fs.unlinkSync(req.file.path);
                }
                res.status(400).json({ error: 'Error parsing CSV' });
            })
            .on('end', async () => {
                try {
                    if (rows.length === 0) {
                        fs.unlinkSync(req.file.path);
                        return res.status(400).json({ error: 'CSV vacío' });
                    }
                    
                    let insertedCount = 0;
                    let skippedCount = 0;
                    let errors = [];

                    for (const row of rows) {
                        try {
                            if (!row.id || !row.transaction_id || !row.customer_id || !row.product_id) {
                                skippedCount++;
                                errors.push(`Fila sin datos requeridos: ${JSON.stringify(row)}`);
                                continue;
                            }

                            // Validar customer_id
                            const custCheck = await pool.query(
                                `SELECT id FROM ${TABLA4_nombre} WHERE id = $1`,
                                [row.customer_id]
                            );
                            if (custCheck.rows.length === 0) {
                                skippedCount++;
                                errors.push(`customer_id ${row.customer_id} no existe`);
                                continue;
                            }

                            // Validar product_id
                            const prodCheck = await pool.query(
                                `SELECT id FROM ${TABLA5_nombre} WHERE id = $1`,
                                [row.product_id]
                            );
                            if (prodCheck.rows.length === 0) {
                                skippedCount++;
                                errors.push(`product_id ${row.product_id} no existe`);
                                continue;
                            }

                            // Validar supplier_id si existe
                            if (row.supplier_id) {
                                const suppCheck = await pool.query(
                                    `SELECT id FROM ${TABLA6_nombre} WHERE id = $1`,
                                    [row.supplier_id]
                                );
                                if (suppCheck.rows.length === 0) {
                                    skippedCount++;
                                    errors.push(`supplier_id ${row.supplier_id} no existe`);
                                    continue;
                                }
                            }
                            
                            await pool.query(
                                `INSERT INTO ${TABLA7_nombre} (id, transaction_id, date, customer_id, product_id, quantity, total_line_value, supplier_id) 
                                VALUES ($1, $2, $3, $4, $5, $6, $7, $8) ON CONFLICT (id) DO NOTHING`,
                                [row.id, row.transaction_id, row.date || null, row.customer_id, row.product_id, row.quantity || null, row.total_line_value || null, row.supplier_id || null]
                            );
                            insertedCount++;
                        } catch (err) {
                            console.warn('Error insertando fila:', err.message);
                            skippedCount++;
                            errors.push(err.message);
                        }
                    }

                    await saveLog('INSERT', TABLA7_nombre, insertedCount);
                    fs.unlinkSync(req.file.path);
                    
                    res.json({ 
                        ok: true, 
                        total: rows.length, 
                        inserted: insertedCount,
                        skipped: skippedCount,
                        errors: errors.length > 0 ? errors : null
                    });
                } catch (error) {
                    console.error('Error final:', error);
                    if (req.file && fs.existsSync(req.file.path)) {
                        fs.unlinkSync(req.file.path);
                    }
                    res.status(500).json({ error: 'Error procesando datos', details: error.message });
                }
            });
    } catch (error) {
        console.error('Error:', error);
        if (req.file && fs.existsSync(req.file.path)) {
            fs.unlinkSync(req.file.path);
        }
        res.status(500).json({ error: 'Error al procesar archivo', details: error.message });
    }
});

app.get(`/${TABLA7_nombre}`, async (req, res) => {
    try {
        const result = await pool.query(`
            SELECT t.id, t.transaction_id, t.date, t.customer_id, c.name as customer_name, 
                   t.product_id, p.name as product_name, t.quantity, t.total_line_value, 
                   t.supplier_id, s.name as supplier_name, t.created_at
            FROM ${TABLA7_nombre} t
            LEFT JOIN ${TABLA4_nombre} c ON t.customer_id = c.id
            LEFT JOIN ${TABLA5_nombre} p ON t.product_id = p.id
            LEFT JOIN ${TABLA6_nombre} s ON t.supplier_id = s.id
            ORDER BY t.created_at DESC
        `);
        res.json(result.rows);
    } catch (error) {
        res.status(500).json({ error: error.message });
    }
});

// ==================== ENDPOINT: HEALTH CHECK ====================
app.get('/health', (req, res) => {
    res.json({ status: 'ok', message: 'Servidor ejecutándose' });
});

// ==================== INICIAR SERVIDOR ====================
const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
    console.log(`
╔════════════════════════════════════════════╗
║  🚀 Servidor en http://localhost:${PORT}       ║
╠════════════════════════════════════════════╣
║  📤 ENDPOINTS DE CARGA:                   ║
║  POST /api/upload/category                ║
║  POST /api/upload/city                    ║
║  POST /api/upload/sku                     ║
║  POST /api/upload/customer                ║
║  POST /api/upload/product                 ║
║  POST /api/upload/suppliers               ║
║  POST /api/upload/transaction             ║
║                                           ║
║  📥 ENDPOINTS DE LECTURA:                 ║
║  GET  /category                           ║
║  GET  /city                               ║
║  GET  /sku                                ║
║  GET  /customer                           ║
║  GET  /product                            ║
║  GET  /suppliers                          ║
║  GET  /transaction                        ║
║                                           ║
║  💚 HEALTH:                               ║
║  GET  /health                             ║
╚════════════════════════════════════════════╝
    `);
});

process.on('SIGINT', async () => {
    console.log('\n🔴 Cerrando servidor...');
    await pool.end();
    process.exit(0);
});
