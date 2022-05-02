const dotenv = require('dotenv');
const sql = require("mssql");
const {BigQuery} = require('@google-cloud/bigquery');
//-- uncomment during local development
const express = require('express');
const app = express();
// uncomment during local development


/**
 * Responds to any HTTP request.
 *
 * @param {!express:Request} req HTTP request context.
 * @param {!express:Response} res HTTP response context.
 */
//exports.chk_item_qty_onorder_openpurch = 
app.get('/', 
async (req, res) => {
  const BQ_LOCATION = 'asia-east1';
  const   DatasetID = 'SAP_chk';
  const     TableID = 'chk_item_qty_onorder_openpurch';

  dotenv.config();

  let sqlConfig = {
          user: process.env.user,
      password: process.env.password,
      database: process.env.database,
        server: process.env.server,
        trustServerCertificate: true,
  };

  let sqlQ = `
    WITH
    T99 AS (--品項主檔已訂貨數量
    SELECT
      T0.ItemCode,
      SUM(T0.OnOrder) AS 'Total_OnOrder'
    FROM
      OITW T0
    WHERE
      T0.OnOrder <> 0
    GROUP BY
      T0.ItemCode
    ),
    
    T98 AS (--未結採購數量
    SELECT
      T0.ItemCode,
      SUM(T0.OpenQty) AS 'Total_OpenPur'
    FROM
      POR1 T0
    WHERE
      T0.OpenQty <> 0
      AND  T0.ItemCode NOT IN ('00002', '00003')
    GROUP BY
      T0.ItemCode
    ),
    
    T97 AS (--未結調撥數量
    SELECT
      T0.ItemCode,
      SUM(T0.OpenCreQty) AS 'Total_OpenTransfer'
    FROM
      WTQ1 T0
    WHERE
      T0.OpenCreQty <> 0
    GROUP BY
      T0.ItemCode
    )
        
    --彙總計算差異
    SELECT
      T99.ItemCode,
      ISNULL(T99.Total_OnOrder, 0) AS 'Total_OnOrder',
      ISNULL(T98.Total_OpenPur, 0) AS 'Total_OpenPur',
      ISNULL(T97.Total_OpenTransfer, 0) AS 'Total_OpenTransfer',
      ISNULL(T99.Total_OnOrder, 0) - ISNULL(T98.Total_OpenPur, 0) - ISNULL(T97.Total_OpenTransfer, 0) AS 'DIFF'
    FROM
      T99
      FULL JOIN T98 ON T98.ItemCode = T99.ItemCode
      FULL JOIN T97 ON T97.ItemCode = T99.ItemCode
--    WHERE
--      ISNULL(T99.Total_OnOrder, 0) - ISNULL(T98.Total_OpenPur, 0) - ISNULL(T97.Total_OpenTransfer, 0) <> 0
  `;

  try {
    await sql.connect(sqlConfig);
    rs = await sql.query(sqlQ);

    //-- write log and write the record into BQ if any duplicate order is detected
    if ( rs['recordset'].length ) {
      //-- detected the record whose DIFF > 0 
      let diff_items = [];
      
      rs['recordset'].forEach(e => {
if (e.ItemCode == 'CS-23-00040') { 
  e.DIFF = 1; 
  diff_items.push(e);
  console.info( JSON.stringify(e) );
}
      });

      //-- write an error log
      if (0 < diff_items.length) {
        var errLog = {};
        errLog.error = 'item quantity check error';
        errLog.message = 'an item\'s total OnOrder - total OpenQty - total OpenCreQty is not zero';
        errLog.recordset = diff_items;

        console.error( JSON.stringify(errLog) );
      }

      //-- write item quantity info into BigQuery
      const bq = new BigQuery();

      //   dataset
      let opts = {
          location: BQ_LOCATION,
      };
      await bq_getOrCreateDataset(DatasetID, opts);

      //   table schema
      schema = [
        {name: 'ItemCode',           type: 'STRING'},
        {name: 'Total_OnOrder',      type: 'INTEGER'},
        {name: 'Total_OpenPur',      type: 'INTEGER'},
        {name: 'Total_OpenTransfer', type: 'INTEGER'},
        {name: 'DIFF',               type: 'INTEGER'},      
      ];
      opts = {
        schema: schema,
        location: BQ_LOCATION,
      };

      //   tmp table suffix
      let utc8_dt = new Date().getTime() + 8*60*60 * 1000;
      let utc8_str = new Date(utc8_dt).toISOString().substr(0,19).replace(/[T\-:]/g, '');
      let tmpTableId = TableID.concat('_', utc8_str);
      table = await bq_getOrCreateTable(tmpTableId, DatasetID, opts);

      //   insert duplicate order records into tmp table
      await bq.dataset(DatasetID)
        .table(tmpTableId)
        .insert( rs['recordset'] );

      await bq_delTable(TableID, DatasetID);

      //   copy tmp table to regular table 
      opts = {
        query: `CREATE TABLE \`czechrepublic-290206.${DatasetID}.${TableID}\` AS SELECT * FROM \`czechrepublic-290206.${DatasetID}.${tmpTableId}\`;`,
        location: BQ_LOCATION,
      };
      const [job] = await bq.createQueryJob(opts);
      console.log(`Jon ${job.id} started to copy table from ${DatasetID}.${tmpTableId} to ${DatasetID}.${TableID}.`);
      const [rows] = await job.getQueryResults();
      console.log(`Job ${job.id} done.`);
      console.log(`The item quantity info have wrote to BigQuery: ${DatasetID}.${TableID}`);

      //   delete tmp table
      await bq_delTable(tmpTableId, DatasetID);
    }
    else {
      console.info('Found no item.');
    }
  }
  catch (err) {
      console.error(err);
  }
  
  let res_content = 'Check item quantity equality according to OnOrder, OpenPurchase, and OpenTransfer complete.';
  console.info(res_content);
  res.status(200).send(res_content);  
});


//-- uncomment during local development
var server = app.listen(80, function () {
   var host = server.address().address;
   var port = server.address().port;
   console.log("app listening at http://%s:%s", host, port);
});
// uncomment during local development


function get_str_BegDate() {
    //-- get begin date,i.e., today - 3 months
    beg_dt = new Date(Date.now() - 3*30*24*60*60 * 1000 + 8*60*60 * 1000);    
    initData_dt = new Date('2022-02-01');
    beg_dt = (beg_dt < initData_dt) ? initData_dt : beg_dt;    
    return beg_dt.toISOString().split('.')[0];
}

async function bq_getOrCreateDataset(datasetId, opts) {
    const bq = new BigQuery();

    var dataset;
    try {
        [hasDataset] = await bq.dataset(datasetId).exists();
        if (hasDataset) {
            [dataset] = await bq.dataset(datasetId).get();
        }
        else {
            [dataset] = await bq.createDataset(datasetId, opts);
            console.log(`BigQuery dataset ${dataset.id} created.`);
        }
    }
    catch (err) {
        console.error(err);
    }

    return dataset;
}

async function bq_getOrCreateTable(tableId, datasetId, opts) {
    const bq = new BigQuery();

    var table;
    try {
        [hasTable] = await bq.dataset(datasetId).table(tableId).exists();
        if (hasTable) {
            [table] = await bq.dataset(datasetId).table(tableId).get();
        }
        else {
            [table] = await bq
                .dataset(datasetId)
                .createTable(tableId, opts);

            console.log(`Table ${table.id} created.`);
        }
    }
    catch (err) {
        console.error(err);
    }

    return table;
}

async function bq_delTable(tableId, datasetId) {
    const bq = new BigQuery();

    try {
        [hasTable] = await bq.dataset(datasetId).table(tableId).exists();
        if (hasTable) {
            await bq.dataset(datasetId).table(tableId).delete();
            console.log(`Table ${tableId} deleted.`);
        }
        else {
            console.log(`Table ${tableId} does not exist.`);
        }
    }
    catch (err) {
        console.error(err);
    }
}
