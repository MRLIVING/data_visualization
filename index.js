var CONF = { 
  'redis_host': 'ms-payment.mrl.com.tw',
  'fs_collection': 'mrl_magento2_Transactions_forEC',  
  'VALID_INPUT_FILE_PREFIX': 'schema/product_',
  'PATH_GCS_OUTPUT': 'tmp/', // 'ec_data_csv/output/',
  'BQ_DATASET': 'ecount',
};

const {Storage} = require('@google-cloud/storage');
const {Firestore} = require('@google-cloud/firestore');
const {BigQuery} = require('@google-cloud/bigquery');
const readline = require('readline');
const csv = require('csv-parser')
const stripBom = require('strip-bom-stream');
const moment = require('moment');
const path = require('path');
const Objs2CSV = require('objects-to-csv');


/**
 * Triggered from a change to a Cloud Storage bucket.
 *
 * @param {!Object} event Event payload.
 * @param {!Object} context Metadata for the event.
 */
//exports.transFilter = (event, context, callback) => {
//    console.log(`Event Type: ${context.eventType}`);

//    const file = event;
    var bucketName   = 'ecount';
    var pathFileName = 'schema/product_20200226.csv';

    console.log(`gets an event of file: ${pathFileName} in bucket gs://${bucketName}`);
  
    //-- filter valid inputs    
    if (! pathFileName.startsWith(CONF.VALID_INPUT_FILE_PREFIX)) {
        console.log(`an invalid input: ${pathFileName}`);
        //-- termination, see https://cloud.google.com/functions/docs/writing/background#function_parameters
//        callback();
        return 0;
    }
      
    var records = [];
    var csvHeaders = ["product_sku", "product_name"];
    //-- insert data from CSV file into Firestore line by line
    const records_prom = new Promise((resolve, reject) => {
        const storage = new Storage();
        storage
            .bucket(bucketName)
            .file(pathFileName)
            .createReadStream()
            .on('error', function(err) {
                console.error(err);
            })
            .pipe(stripBom())
            .pipe(
                //-- csv-parser, https://www.npmjs.com/package/csv-parser#usage
                csv({
                //-- skip 1st row, ie. "公司名: ..."
                skipLines: 1,
                //-- mapHeaders, https://www.npmjs.com/package/csv-parser#mapheaders
                mapHeaders: ({ header, index }) => {
                    let h = csvHeaders[index];
                    return (h) ? h : null;
                }
            }))
            .on('data', (data) => {
                records.push(data);
            })
            .on('end', () => {
                console.log(`The ECOUNT product CSV is found in GCS: ${'gs://' + bucketName + '/' +  pathFileName}`);
                resolve(records);
            });

        return records;
    })
    .catch(err => {
        console.error(err);
    });

    records_prom.then(async (rs) => {
//        console.log(rs); 

        const products_csv = await new Objs2CSV(rs).toString(header=true);
//        console.log(products_csv);

        const pathFN_products_csv = 'schema/tmp/' + path.basename(pathFileName).split('_')[0] + '.bq.csv';

        const storage = new Storage();
        return storage.bucket(bucketName)
            .file(pathFN_products_csv)
            .save(products_csv)
            .then(() => {
                console.log(`products CSV to BQ has been saved in ${'gs://' + bucketName + '/' + pathFN_products_csv}`);
                return pathFN_products_csv;
            })
            .catch(err => console.error(err));
    })
    .then(pathFN_products_csv => {
//        console.log(pathFN_products_csv);

        const bq = new BigQuery();
        const tbName = path.basename(pathFileName, '.csv');
        const storage = new Storage();
        const metadata = {
            sourceFormat: 'CSV',
            skipLeadingRows: 1,
            schema: {
                fields: [
                    {name: csvHeaders[0], type: 'STRING'},
                    {name: csvHeaders[1], type: 'STRING'},
                ]
            },
            writeDisposition: 'WRITE_TRUNCATE',
        };

        bq.dataset(CONF.BQ_DATASET)
            .table(tbName)
            .load(storage.bucket(bucketName).file(pathFN_products_csv), metadata)
            .then(resp => {
                console.log(resp);
            })
            .catch(err => {
                console.error(err);
            });
    });
//
      

//    //-- Dataset.exists
//    //   https://googleapis.dev/nodejs/bigquery/latest/Dataset.html#exists
//    let ds = bq.dataset(CONF.BQ_DATASET);
//    ds = (! (await ds.exists())[0]) ? (await bq.createDataset(CONF.BQ_DATASET))[0] : ds;
//
//    //-- Dataset.CreateTable if the table doesn't exists
//    //   https://googleapis.dev/nodejs/bigquery/latest/Dataset.html#createTable
//    const tbName = path.basename(pathFileName, '.csv');
//    let tb = ds.table(tbName);
//    let header_str = csvHeaders.join(','); 
//    const options = { 
//        schema: header_str
//    };        
//    tb = (! (await tb.exists())[0]) ? (await ds.createTable(tbName, options))[0] : tb; 
//
//    let csvHeader_trans = trans.map(t => {
//        let tt = Object.keys(t)
//        .filter(k => csvHeaders.includes(k))
//        .reduce((obj, k) => {
//            obj[k] = t[k];
//            return obj; 
//        }, {});
//
//        return tt;
//    }).map(t => {
//        let row = {
//            insertId: t.index_ooid + '_' + t.item_code,
//            json: t
//        };
//
//        return row;                
//    });
//
//    //-- inserts rows with insertId
//    //   https://googleapis.dev/nodejs/bigquery/latest/Table.html#insert
//    tb.insert(csvHeader_trans, 
//        {
//            raw: true
//        }
//    )
//    .then(resp => {
//        console.log(resp[0]);
//    })
//    .catch(err => {
//        console.error(err);
//    });


//    console.log('main thread has run to the end');    
//    return save2bq;
// };

/**
 *  get date from filename, e.g. Transactions_forEC_20191230.csv
 */
function filename2dateStr(fname) {
    let yyyymmdd = fname.match(/_\d+\.csv/g)[0].split('_')[1].split('.csv')[0];
    let dt_str = moment(yyyymmdd + ' 00:00:00', 'YYYYMMDD HH:mm:ss');
    dt_str = dt_str.format('YYYY-MM-DD HH:mm:ss');

    return dt_str;
}
