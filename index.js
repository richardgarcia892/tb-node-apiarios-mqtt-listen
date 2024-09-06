const express = require('express');
const mqtt = require('mqtt');
const { Pool } = require('pg');
const bodyParser = require('body-parser');
const ExcelJS = require('exceljs');

const app = express();
app.use(bodyParser.json());

const pool = new Pool({
  connectionString: 'postgresql://sdapiariosuser:ZxphqHJgJZRa8AO6rsqnFspgRIEKU0bN@dpg-crbulitsvqrc73f4a2sg-a.oregon-postgres.render.com/sdapiarios',
  ssl: {
    rejectUnauthorized: false,
  },
});

// Check database connection
pool.connect((err, client, release) => {
  if (err) {
    console.error('Error acquiring client', err.stack);
  } else {
    console.log('Database connected successfully');
  }
  release();
});

const client = mqtt.connect('mqtt://mqtt:proyecto4Pi@apiarios.cl:1883');

client.on('connect', () => {
  console.log('MQTT broker connected successfully');
  client.subscribe('/pucv/apiarios/esp/estacion');
  client.subscribe('/v2/pucv/apiarios/esp/estacion');
  client.subscribe('/pucv/apiarios/esp/colmena');
  client.subscribe('/v2/pucv/apiarios/esp/colmena');
});

client.on('message', async (topic, message) => {
  console.log(`Received message on topic ${topic}: ${message.toString()}`);
  const payload = JSON.parse(message.toString());
  const fecha_recibido = new Date();
  const fecha_payload = new Date(parseInt(payload.tiempo));

  let query = '';
  let values = [];

  if (topic === '/pucv/apiarios/esp/estacion') {
    query = `INSERT INTO estaciones_v1 (fecha_recibido, fecha_payload, id_estacion, tiempo, temperatura, humedad, presion, velocidad_viento, direccion_viento, lluvia, uv, voltaje_bateria)
                 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)`;
    values = [
      fecha_recibido,
      fecha_payload,
      payload.IDMeteo,
      payload.tiempo,
      payload.T,
      payload.H,
      payload.Pres,
      payload.VelVto,
      payload.DirVto,
      payload.Lluvia,
      payload.UV,
      payload.Vbat,
    ];
  } else if (topic === 'v2/pucv/apiarios/esp/estacion') {
    query = `INSERT INTO estaciones_v2 (fecha_recibido, fecha_payload, id_estacion, tiempo, temperatura, humedad, presion, velocidad_viento, direccion_viento, lluvia, lluvia_total, lluvia_last, uv)
                 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)`;
    values = [
      fecha_recibido,
      fecha_payload,
      payload.IDMeteo,
      payload.tiempo,
      payload.T,
      payload.H,
      payload.P,
      payload.Vel,
      payload.Dir,
      payload.Pluv,
      payload.PluvTot,
      payload.PluvLast,
      payload.UV,
    ];
  } else if (topic === '/pucv/apiarios/esp/colmena') {
    query = `INSERT INTO colmenas_v1 (fecha_recibido, fecha_payload, id_estacion, id_colmena, tiempo, temperatura, humedad, vibracion_x, vibracion_y, vibracion_z, lluvia, voltaje_bateria)
                 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)`;
    values = [
      fecha_recibido,
      fecha_payload,
      payload.IDMeteo,
      payload.Ncol,
      payload.tiempo,
      payload.T,
      payload.H,
      payload.VibX,
      payload.VibY,
      payload.VibZ,
      payload.Pluv,
      payload.Vbat,
    ];
  } else if (topic === 'v2/pucv/apiarios/esp/colmena') {
    query = `INSERT INTO colmenas_v2 (fecha_recibido, fecha_payload, id_estacion, id_colmena, tiempo, temperatura, humedad, vibracion_x, lluvia, voltaje_bateria)
                 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)`;
    values = [fecha_recibido, fecha_payload, payload.IDMeteo, payload.Ncol, payload.tiempo, payload.T, payload.H, payload.VibX, payload.Pluv, payload.Vbat];
  }

  try {
    await pool.query(query, values);
    console.log('Data inserted successfully');
  } catch (err) {
    console.error('Error inserting data', err);
  }
});

app.get('/excel/:table', async (req, res) => {
  const table = req.params.table;
  try {
    const result = await pool.query(`SELECT * FROM ${table}`);
    console.log(`Data fetched successfully from ${table}`);

    const workbook = new ExcelJS.Workbook();
    const worksheet = workbook.addWorksheet(table);

    // Define columns
    worksheet.columns = [
      { header: 'fecha_recibido', key: 'fecha_recibido', width: 20 },
      { header: 'fecha_payload', key: 'fecha_payload', width: 20 },
      { header: 'id_estacion', key: 'id_estacion', width: 10 },
      { header: 'id_colmena', key: 'id_colmena', width: 10 },
      { header: 'tiempo', key: 'tiempo', width: 20 },
      { header: 'temperatura', key: 'temperatura', width: 15 },
      { header: 'humedad', key: 'humedad', width: 15 },
      { header: 'presion', key: 'presion', width: 15 },
      { header: 'velocidad_viento', key: 'velocidad_viento', width: 20 },
      { header: 'direccion_viento', key: 'direccion_viento', width: 20 },
      { header: 'lluvia', key: 'lluvia', width: 15 },
      { header: 'lluvia_total', key: 'lluvia_total', width: 15 },
      { header: 'lluvia_last', key: 'lluvia_last', width: 15 },
      { header: 'uv', key: 'uv', width: 10 },
      { header: 'voltaje_bateria', key: 'voltaje_bateria', width: 20 },
      { header: 'vibracion_x', key: 'vibracion_x', width: 20 },
      { header: 'vibracion_y', key: 'vibracion_y', width: 20 },
      { header: 'vibracion_z', key: 'vibracion_z', width: 20 },
    ];

    // Add rows
    worksheet.addRows(result.rows);

    // Format date-time columns
    worksheet.getColumn('fecha_recibido').eachCell((cell) => {
      cell.numFmt = 'DD-MM-YYYY hh:mm:ss';
    });
    worksheet.getColumn('fecha_payload').eachCell((cell) => {
      cell.numFmt = 'DD-MM-YYYY hh:mm:ss';
    });

    // Set response headers
    res.setHeader('Content-Type', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet');
    res.setHeader('Content-Disposition', `attachment; filename=${table}.xlsx`);

    // Write to response
    await workbook.xlsx.write(res);
    res.end();
  } catch (err) {
    console.error('Error fetching data', err);
    res.status(500).send('Error generating Excel file');
  }
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
});
