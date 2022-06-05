const dotenv = require('dotenv');
const mysql = require('mysql');
const express = require('express');
const app = express();
const sql = require("mssql");
const {BigQuery} = require('@google-cloud/bigquery');



/**
 * Responds to any HTTP request.
 *
 * @param {!express:Request} req HTTP request context.
 * @param {!express:Response} res HTTP response context.
 */
app.get('/', 
async (req, res) => {
    const BQ_LOCATION = 'asia-east1';
    const DatasetID = 'SAP_chk';
    const M2_Order_TableID = 'chk_mysql_sales_order';
    const MS_Order_TableID = 'chk_mssql_ordr';
    const MS_IncomePay_TableID = 'chk_mssql_orct';

    dotenv.config();
    let mysqlConfig = {
            host: process.env.mysql_server,
            user: process.env.mysql_user, 
        password: process.env.mysql_password,
        database: process.env.mysql_database,
    };

    let my_conn = await mysql.createConnection(mysqlConfig); 

    let order_mySqlQ = `
        SELECT 
            increment_id, 
            STATUS, 
            grand_total,
            created_at,
            updated_at
        FROM 
            sales_order
        WHERE
            STATUS in ('processing', 'closed', 'complete')
            AND increment_id IS NOT NULL
            AND '${get_str_nMonthAgoDate(3)}' <= created_at;
    `;
  
    let opts = {
        location: BQ_LOCATION,
    };

    schema = [
        {name: 'increment_id', type: 'STRING', mode: 'REQUIRED'},
        {name: 'status', type: 'STRING'},
        {name: 'grand_total', type: 'NUMERIC'},
        {name: 'created_at', type: 'TIMESTAMP'},
        {name: 'updated_at', type: 'TIMESTAMP'},
    ];

    opts = {
        schema: schema,
        location: BQ_LOCATION,
    };

    await bq_getOrCreateDataset(DatasetID, opts);            

    //-- M2 tmp table
    let dt = new Date().getTime();
    let m2Order_tt = M2_Order_TableID.concat('_', dt);
    table = await bq_getOrCreateTable(m2Order_tt, DatasetID, opts);
    bq_selectInsertTable_mysql(order_mySqlQ, m2Order_tt, schema, opts, DatasetID, my_conn);


    let mssqlConfig = {
            user: process.env.mssql_user,
        password: process.env.mssql_password, 
        database: process.env.mssql_database,
          server: process.env.mssql_server,
          trustServerCertificate: true,
    };
    await sql.connect(mssqlConfig);

    let order_msSqlQ = `
        SELECT
            NumAtCard,
            DocStatus,
            DocTotal,
            DocDate
        FROM 
            ORDR
        WHERE 
            CANCELED = 'N' 
            AND NumAtCard IS NOT NULL 
        ;
    `;


    //   table schema
    schema = [
        {name: 'NumAtCard', type: 'STRING', mode: 'REQUIRED'},
        {name: 'DocStatus', type: 'STRING'},
        {name: 'DocTotal', type: 'NUMERIC'},
        {name: 'DocDate', type: 'TIMESTAMP'},
    ];

    opts = {
        schema: schema,
        location: BQ_LOCATION,
    };

    //-- SAP ORDR tmp table
    dt = new Date().getTime();
    sapORDR_tt = MS_Order_TableID.concat('_', dt);
    await bq_selectInsertTable_mssql(order_msSqlQ, sapORDR_tt, schema, opts, DatasetID, sql)


    let incomePay_msSqlQ = `
        SELECT 
            CounterRef, 
--            SUM(DocTotal) as payTotal 
            SUM(CashSum) AS payTotal
        FROM 
            ORCT 
        WHERE 
            CANCELED = 'N'
            AND CounterRef IS NOT NULL 
        GROUP BY 
            CounterRef
        ;
    `;

    incomePay_schema = [
        {name: 'CounterRef', type: 'STRING'},
        {name: 'payTotal', type: 'NUMERIC'},
    ];

    incomePay_opts = {
        schema: incomePay_schema,
        location: BQ_LOCATION,
    };

    //-- SAP ORCT tmp table
    dt = new Date().getTime();
    sapORCT_tt = MS_IncomePay_TableID.concat('_', dt);
    await bq_selectInsertTable_mssql(incomePay_msSqlQ, sapORCT_tt, incomePay_schema, incomePay_opts, DatasetID, sql)


    let join_bqSqlQ = `
        DROP TABLE IF EXISTS
          ${DatasetID}.chk_amount_orders_m2order_ordr_orct;

        CREATE TABLE
          ${DatasetID}.chk_amount_orders_m2order_ordr_orct AS 
        SELECT
          increment_id,
          NumAtCard,
          CounterRef,
          grand_total as m2_grandTotal,
          DocTotal as sap_ordr_docTotal,
          payTotal as sap_orct_payTotal,
          created_at as m2_create_ts,
          updated_at as m2_update_ts
        FROM
          ${DatasetID}.${m2Order_tt} as sales_order
        LEFT JOIN
          ${DatasetID}.${sapORDR_tt} as ordr
        ON
          increment_id = NumAtCard
        LEFT JOIN
          ${DatasetID}.${sapORCT_tt} as orct
        ON
          NumAtCard = CounterRef
        ;
    `;

    opts = {
      query: join_bqSqlQ,
//      destination: destinationTable,
      location: BQ_LOCATION,
    };
 
    await bq_queryJob(join_bqSqlQ, opts);

    await bq_delTable(m2Order_tt, DatasetID);
    await bq_delTable(sapORDR_tt, DatasetID);
    await bq_delTable(sapORCT_tt, DatasetID);

    res.status(200).send('generates amount of orders completely');
}
);

function get_str_nMonthAgoDate(n_month) {
    //-- get begin date,e.g, today - 3 months where n_month = 3
    beg_dt = new Date(Date.now() - n_month*30*24*60*60*1000 + 8*60*60*1000);
    initData_dt = new Date('2022-02-01');
    beg_dt = (beg_dt < initData_dt) ? initData_dt : beg_dt;

    //--  ISOString, e.g., 2022-06-05T10:50:25.924Z
    return beg_dt.toISOString().split('T')[0];
}

async function bq_selectInsertTable_mssql(sqlQ, tableId, schema, opts, datasetId, sqlConn) {
    let rt = false;

    try {
        //-- get query result, e.g., recoedset
        //   see https://www.npmjs.com/package/mssql 
        rs = await sqlConn.query(sqlQ);

        if ( rs['recordset'].length ) {
            await bq_getOrCreateDataset(datasetId, opts);

            //-- create table if not exists
            table = await bq_getOrCreateTable(tableId, datasetId, opts);
           
            //--  insert the records into tmp table
            const bq = new BigQuery();
            await bq.dataset(datasetId)
                .table(tableId)
                .insert( rs['recordset'] );

            console.log(`BigQuery Select & Insert to ${datasetId}.${tableId} completion.`);
        }

        rt = true;
    }
    catch (err) {
        console.error(err);
    }

    return rt;
}

async function bq_selectInsertTable_mysql(sqlQ, tableId, schema, opts, datasetId, sqlConn) {
    let rt = false;

    try {
        //-- get query result, e.g., recoedset
        //   see https://www.npmjs.com/package/mysql
        let records = await new Promise((resolve, reject) => {
            sqlConn.query(sqlQ, (err, rs, fields) => {
                (err) ? reject(err) : resolve(rs)
            })
        });

        let recs_unpack_json = 
            records.map((r, i) => Object.assign({}, r) );
//        console.log(recs_unpack_json);

        if ( records.length ) {
            await bq_getOrCreateDataset(datasetId, opts);

            //-- create table if not exists
            table = await bq_getOrCreateTable(tableId, datasetId, opts);
           
            //--  insert the records into tmp table
            const bq = new BigQuery();
            await bq.dataset(datasetId)
                .table(tableId)
                .insert(recs_unpack_json);

            console.log(`BigQuery Select & Insert to ${datasetId}.${tableId} completion.`);
        }

        rt = true;
    }
    catch (err) {
        console.error(err);
    }

    return rt;
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

async function bq_queryJob(sql, opts) {
    //-- Retrieve a table's rows, see more from following links
    //   https://github.com/googleapis/nodejs-bigquery/blob/main/samples/browseTable.js
    //   https://cloud.google.com/bigquery/docs/reference/rest/v2/Job#jobconfigurationquery
    try {
        const bigquery = new BigQuery();
        const [job] = await bigquery.createQueryJob(opts);

        //-- see https://cloud.google.com/bigquery/docs/reference/v2/jobs/getQueryResults
        const queryResultsOptions = {
            // retrieve zero resulting rows.
            maxResults: 0,
        };

        //-- wait for the job to finish.
        await job.getQueryResults(queryResultsOptions);
    }
    catch (err) {
        console.error(err);
    }
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


var server = app.listen(80, function () {
   var host = server.address().address;
   var port = server.address().port;
   console.log("app listening at http://%s:%s", host, port);
});


