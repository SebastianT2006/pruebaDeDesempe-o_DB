// ==================== IMPORTAR LIBRERÍAS ====================
const express = require('express');        // Para crear servidor web
const multer = require('multer');          // Para recibir archivos
const { parse } = require('csv-parse');    // Para leer CSV
const { Pool } = require('pg');            // Para conectar PostgreSQL
const fs = require('fs');                  // Para manejar archivos
//const { MongoClient } = require('mongodb'); // Para conectar MongoDB

// ==================== CREAR SERVIDOR ====================
const app = express();
const upload = multer({ dest: 'uploads/' }); // Guardar en carpeta uploads/

// ==================== CONEXIÓN POSTGRESQL ====================
const pool = new Pool({
    user: 'postgres',          // ← CAMBIA
    host: 'localhost',             // ← CAMBIA
    database: 'postgres',             // ← CAMBIA
    password: 'password', // ← CAMBIA
    port: 5434,
});

// ==================== CONEXIÓN MONGODB ====================
/*const uri = 'mongodb://localhost:27017';
const client = new MongoClient(uri);
let logsCollection;

async function connectDB() {
    try {
        await client.connect();
        const db = client.db('app');
        logsCollection = db.collection('logs');
        console.log('✓ MongoDB');
    } catch (error) {
        console.error('Error:', error);
    }
}
connectDB();*/

/*async function saveLog(action, table, count) {
    try {
        await logsCollection.insertOne({ action, table, count, created_at: new Date() });
    } catch (error) {
        console.error('Error:', error);
    }
}*/

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
        // TABLA 1 (PADRE)
        await pool.query(`
            CREATE TABLE IF NOT EXISTS ${TABLA1_nombre} (
                id VARCHAR(50) PRIMARY KEY,
                name VARCHAR(255) NOT NULL UNIQUE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        `);
        console.log(`✓ ${TABLA1_nombre}`);

        // TABLA 2 (FK a tabla1)
        await pool.query(`
            CREATE TABLE IF NOT EXISTS ${TABLA2_nombre} (
                id VARCHAR(50) PRIMARY KEY,
                name VARCHAR(255) NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                
            );
        `);
        console.log(`✓ ${TABLA2_nombre}`);

                // TABLA 3 
        await pool.query(`
            CREATE TABLE IF NOT EXISTS ${TABLA3_nombre} (
                id VARCHAR(50) PRIMARY KEY,
                name VARCHAR(255) NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        `);
        console.log(`✓ ${TABLA3_nombre}`);

                // TABLA 4 (FK a tabla2)
        await pool.query(`
            CREATE TABLE IF NOT EXISTS ${TABLA4_nombre} (
                id VARCHAR(50) PRIMARY KEY,
                name VARCHAR(255) NOT NULL,
                lastname VARCHAR(255) NOT NULL,
                ${TABLA2_nombre}_id VARCHAR(50) NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (${TABLA2_nombre}_id) REFERENCES ${TABLA2_nombre}(id) ON DELETE CASCADE
            );
        `);
        console.log(`✓ ${TABLA4_nombre}`);

         // TABLA 5 (FK a tabla1 y tabla3)
        await pool.query(`
            CREATE TABLE IF NOT EXISTS ${TABLA5_nombre} (
                id VARCHAR(50) PRIMARY KEY,
                ${TABLA1_nombre}_id VARCHAR(50) NOT NULL,
                ${TABLA3_nombre}_id VARCHAR(50) NOT NULL,
                name VARCHAR(255) NOT NULL,
                unit_price FLOAT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (${TABLA1_nombre}_id) REFERENCES ${TABLA1_nombre}(id) ON DELETE CASCADE,
                 FOREIGN KEY (${TABLA3_nombre}_id) REFERENCES ${TABLA3_nombre}(id) ON DELETE CASCADE
            );
        `);
        console.log(`✓ ${TABLA5_nombre}`);

        // TABLA 6(FK a tabla1 y tabla3)
        await pool.query(`
            CREATE TABLE IF NOT EXISTS ${TABLA6_nombre} (
                id VARCHAR(50) PRIMARY KEY,
                name VARCHAR(255) NOT NULL,
                email VARCHAR(255) NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                
            );
        `);
        console.log(`✓ ${TABLA6_nombre}`);
            // TABLA 7
            await pool.query(`
            CREATE TABLE IF NOT EXISTS ${TABLA7_nombre} (
                id VARCHAR(50) PRIMARY KEY,
                transaction_id VARCHAR(255) NOT NULL,
                date DATE,
                ${TABLA4_nombre}_id VARCHAR(50) NOT NULL,
                ${TABLA5_nombre}_id VARCHAR(50) NOT NULL,
                quantity INT,
                unit_price FLOAT,
                total_line_value FLOAT,
                ${TABLA6_nombre}_id VARCHAR(50) NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (${TABLA4_nombre}_id) REFERENCES ${TABLA4_nombre}(id) ON DELETE CASCADE,
                FOREIGN KEY (${TABLA5_nombre}_id) REFERENCES ${TABLA5_nombre}(id) ON DELETE CASCADE,
                FOREIGN KEY (${TABLA6_nombre}_id) REFERENCES ${TABLA6_nombre}(id) ON DELETE CASCADE
            );
        `);
        
    } catch (error) {
        console.error('Error:', error);
    }
}
createTables();
/*
// ==================== ENDPOINT: SUBIR A TABLA1 ====================
app.post(`/api/upload/${TABLA1_nombre}`, upload.single('archivo'), async (req, res) => {
    if (!req.file) return res.status(400).json({ error: 'No file' });
    const rows = [];
    
    try {
        fs.createReadStream(req.file.path)
            .pipe(parse({ columns: true, trim: true }))
            .on('data', row => rows.push(row))
            .on('end', async () => {
                try {
                    if (rows.length === 0) {
                        fs.unlinkSync(req.file.path);
                        return res.status(400).json({ error: 'Vacío' });
                    }
                    
                    let insertedCount = 0;
                    for (const row of rows) {
                        try {
                            if (!row.id || !row.nombre) continue;
                            await pool.query(
                                `INSERT INTO ${TABLA1_nombre} (id, nombre, ciudad) VALUES ($1, $2, $3) ON CONFLICT (id) DO NOTHING`,
                                [row.id, row.nombre, row.ciudad || null]
                            );
                            insertedCount++;
                        } catch (err) {
                            console.warn('Error:', err.message);
                        }
                    }
                    await saveLog('INSERT', TABLA1_nombre, insertedCount);
                    fs.unlinkSync(req.file.path);
                    res.json({ ok: true, total: rows.length, inserted: insertedCount });
                } catch (error) {
                    fs.unlinkSync(req.file.path);
                    res.status(500).json({ error: 'Error' });
                }
            });
    } catch (error) {
        if (req.file && fs.existsSync(req.file.path)) fs.unlinkSync(req.file.path);
        res.status(500).json({ error: 'Error' });
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

// ==================== ENDPOINT: SUBIR A TABLA2 ====================
app.post(`/api/upload/${TABLA2_nombre}`, upload.single('archivo'), async (req, res) => {
    if (!req.file) return res.status(400).json({ error: 'No file' });
    const rows = [];
    
    try {
        fs.createReadStream(req.file.path)
            .pipe(parse({ columns: true, trim: true }))
            .on('data', row => rows.push(row))
            .on('end', async () => {
                try {
                    if (rows.length === 0) {
                        fs.unlinkSync(req.file.path);
                        return res.status(400).json({ error: 'Vacío' });
                    }
                    
                    let insertedCount = 0, skippedCount = 0;
                    for (const row of rows) {
                        try {
                            if (!row.id || !row.nombre || !row[`${TABLA1_nombre}_id`]) {
                                skippedCount++;
                                continue;
                            }
                            
                            const fkCheck = await pool.query(
                                `SELECT id FROM ${TABLA1_nombre} WHERE id = $1`,
                                [row[`${TABLA1_nombre}_id`]]
                            );
                            
                            if (fkCheck.rows.length === 0) {
                                skippedCount++;
                                continue;
                            }
                            
                            await pool.query(
                                `INSERT INTO ${TABLA2_nombre} (id, nombre, ${TABLA1_nombre}_id, descripcion) VALUES ($1, $2, $3, $4) ON CONFLICT (id) DO NOTHING`,
                                [row.id, row.nombre, row[`${TABLA1_nombre}_id`], row.descripcion || null]
                            );
                            insertedCount++;
                        } catch (err) {
                            console.warn('Error:', err.message);
                            skippedCount++;
                        }
                    }
                    await saveLog('INSERT', TABLA2_nombre, insertedCount);
                    fs.unlinkSync(req.file.path);
                    res.json({ ok: true, total: rows.length, inserted: insertedCount, skipped: skippedCount });
                } catch (error) {
                    fs.unlinkSync(req.file.path);
                    res.status(500).json({ error: 'Error' });
                }
            });
    } catch (error) {
        if (req.file && fs.existsSync(req.file.path)) fs.unlinkSync(req.file.path);
        res.status(500).json({ error: 'Error' });
    }
});

app.get(`/${TABLA2_nombre}`, async (req, res) => {
    try {
        const result = await pool.query(`
            SELECT t2.id, t2.nombre, t2.${TABLA1_nombre}_id, t1.nombre as ${TABLA1_nombre}_nombre, t2.descripcion, t2.created_at
            FROM ${TABLA2_nombre} t2
            LEFT JOIN ${TABLA1_nombre} t1 ON t2.${TABLA1_nombre}_id = t1.id
            ORDER BY t2.created_at DESC
        `);
        res.json(result.rows);
    } catch (error) {
        res.status(500).json({ error: error.message });
    }
});

// ==================== ENDPOINT: SUBIR A TABLA3 ====================
app.post(`/api/upload/${TABLA3_nombre}`, upload.single('archivo'), async (req, res) => {
    if (!req.file) return res.status(400).json({ error: 'No file' });
    const rows = [];
    
    try {
        fs.createReadStream(req.file.path)
            .pipe(parse({ columns: true, trim: true }))
            .on('data', row => rows.push(row))
            .on('end', async () => {
                try {
                    if (rows.length === 0) {
                        fs.unlinkSync(req.file.path);
                        return res.status(400).json({ error: 'Vacío' });
                    }
                    
                    let insertedCount = 0, skippedCount = 0;
                    for (const row of rows) {
                        try {
                            if (!row.id || !row.nombre || !row[`${TABLA2_nombre}_id`]) {
                                skippedCount++;
                                continue;
                            }
                            
                            const fkCheck = await pool.query(
                                `SELECT id FROM ${TABLA2_nombre} WHERE id = $1`,
                                [row[`${TABLA2_nombre}_id`]]
                            );
                            
                            if (fkCheck.rows.length === 0) {
                                skippedCount++;
                                continue;
                            }
                            
                            await pool.query(
                                `INSERT INTO ${TABLA3_nombre} (id, nombre, ${TABLA2_nombre}_id, cantidad) VALUES ($1, $2, $3, $4) ON CONFLICT (id) DO NOTHING`,
                                [row.id, row.nombre, row[`${TABLA2_nombre}_id`], row.cantidad || null]
                            );
                            insertedCount++;
                        } catch (err) {
                            console.warn('Error:', err.message);
                            skippedCount++;
                        }
                    }
                    await saveLog('INSERT', TABLA3_nombre, insertedCount);
                    fs.unlinkSync(req.file.path);
                    res.json({ ok: true, total: rows.length, inserted: insertedCount, skipped: skippedCount });
                } catch (error) {
                    fs.unlinkSync(req.file.path);
                    res.status(500).json({ error: 'Error' });
                }
            });
    } catch (error) {
        if (req.file && fs.existsSync(req.file.path)) fs.unlinkSync(req.file.path);
        res.status(500).json({ error: 'Error' });
    }
});

app.get(`/${TABLA3_nombre}`, async (req, res) => {
    try {
        const result = await pool.query(`
            SELECT t3.id, t3.nombre, t3.${TABLA2_nombre}_id, t2.nombre as ${TABLA2_nombre}_nombre, t3.cantidad, t3.created_at
            FROM ${TABLA3_nombre} t3
            LEFT JOIN ${TABLA2_nombre} t2 ON t3.${TABLA2_nombre}_id = t2.id
            ORDER BY t3.created_at DESC
        `);
        res.json(result.rows);
    } catch (error) {
        res.status(500).json({ error: error.message });
    }
});

// ==================== ENDPOINT: VER LOGS ====================
app.get('/logs', async (req, res) => {
    try {
        const logs = await logsCollection.find().sort({ created_at: -1 }).toArray();
        res.json(logs);
    } catch (error) {
        res.status(500).json({ error: error.message });
    }
});

// ==================== INICIAR SERVIDOR ====================
app.listen(3000, () => {
    console.log(`
    🚀 Servidor en http://localhost:3000
    
    POST /api/upload/${TABLA1_nombre}   GET  /${TABLA1_nombre}
    POST /api/upload/${TABLA2_nombre}   GET  /${TABLA2_nombre}
    POST /api/upload/${TABLA3_nombre}   GET  /${TABLA3_nombre}
    GET  /logs
    `);
});

process.on('SIGINT', async () => {
    console.log('\nCerrando...');
    await client.close();
    await pool.end();
    process.exit(0);
});*/