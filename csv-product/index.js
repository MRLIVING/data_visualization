var CONF = {     
  'VALID_INPUT_FILE_PREFIX': 'primary_table/product_',
          'PATH_GCS_OUTPUT': 'tmp/', 
               'BQ_DATASET': 'ecount',
};

const {Storage} = require('@google-cloud/storage');
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
exports.product_table = (event, context) => {
    const gcsEvent = event
    var bucketName   = gcsEvent.bucket;
    var pathFileName = gcsEvent.name;

    console.log(`gets an event of file: ${pathFileName} in bucket gs://${bucketName}`);
  
    //-- filter valid inputs    
    if (! pathFileName.startsWith(CONF.VALID_INPUT_FILE_PREFIX)) {
        console.log(`an invalid input: ${pathFileName}`);
        //-- termination, see https://cloud.google.com/functions/docs/writing/background#function_parameters        
        return 0;
    }
      
    //-- load products and transform to BQ table form from with ECOUNT CSV file
    var records = [];
    var csvHeaders = ["product_sku", "product_name"];
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
    
    let export2bq_prom = records_prom.then(async (rs) => {
//        console.log(rs); 

        const products_csv = await new Objs2CSV(rs).toString(header=true);
//        console.log(products_csv);

        const pathFN_products_csv = CONF.PATH_GCS_OUTPUT + path.basename(pathFileName).split('_')[0] + '.bq.csv';

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

        return bq.dataset(CONF.BQ_DATASET)
            .table(tbName)
            .load(storage.bucket(bucketName).file(pathFN_products_csv), metadata)
            .then(resp => {
                console.log(resp);
            })
            .catch(err => {
                console.error(err);
            });
    });
  
    return export2bq_prom;  
};
