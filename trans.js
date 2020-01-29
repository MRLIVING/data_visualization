var CONF = { 
//  'redis_host': 'ms-payment.mrl.com.tw',
  'redis_host': '10.254.103.59',
  'fs_collection': 'mrl_magento2_Transactions_forEC',
  'PATH_LOCAL_OUTPUT': '/tmp/',
  'PATH_GCS_OUTPUT': 'tmp/',
};

const {Storage} = require('@google-cloud/storage');
const {Firestore} = require('@google-cloud/firestore');
//const {BigQuery} = require('@google-cloud/bigquery');
const readline = require('readline');
const csv = require('csv-parser')
const stripBom = require('strip-bom-stream');
const moment = require('moment');

/**
 * Triggered from a change to a Cloud Storage bucket.
 *
 * @param {!Object} event Event payload.
 * @param {!Object} context Metadata for the event.
 */
//exports.orderFilter = (data, context, callback) => {
//    console.log(`Event Type: ${context.eventType}`);

//    const file = data;
    var bucketName = 'ecount';
    var pathFileName   = 'tmp/Transactions_forEC_20191230.csv';

    var csvHeaders = [];

    //-- insert data from CSV file into Firestore line by line
    const l2fs_prom = new Promise((resolve, reject) => {
        let proms = [];

        let _dt_gcs = filename2dateStr(pathFileName);
        console.log(`uploaded file in GCS: ${_dt_gcs} with filename: ${pathFileName}`);

        const firestore = new Firestore();
        const storage = new Storage();
        storage
            .bucket(bucketName)
            .file(pathFileName)
            .createReadStream()
            .on('error', function(err) {
                console.error(err);
            })
            .pipe(stripBom())
            .pipe(csv())
            .on('headers', (headers) => {
                csvHeaders = headers;
            })
            .on('data', (line) => {
                line._dt_gcs = new Date(_dt_gcs);
                
                //-- set an new document into firestore
                //   firestore.doc(), https://googleapis.dev/nodejs/firestore/latest/Firestore.html#doc
                const docPath = CONF.fs_collection + '/' + line.index_ooid + '_' + line.item_code;
                const doc = firestore.doc(docPath); 
                proms.push( doc.set(line) );
            })
            .on('end', () => {
                resolve(proms);
            })
    })
    .then((writeResults) => {
//        console.log(writeResults);
        //-- wait all transcations set into Firestore
        //   promise.allsettled, https://github.com/es-shims/Promise.allSettled
        let allSettled = require('promise.allsettled');
        return allSettled(writeResults);
    })
    .catch(err => {
        console.error(err);
    });


    const paidTranFSPaths_prom = l2fs_prom.then(() => {
        let _dt_gcs = filename2dateStr(pathFileName);
        let dt_beg = moment(_dt_gcs, 'YYYY-MM-DD HH:mm:ss').add(-30, 'days').toDate();
        let dt_end = moment(_dt_gcs, 'YYYY-MM-DD HH:mm:ss').toDate();
        console.log(`gets transactions between ${dt_beg} and ${dt_end}`);

        return new Firestore()
            .collection(CONF.fs_collection)
            .where('_dt_gcs', '>=', dt_beg)
            .where('_dt_gcs', '<=', dt_end)
            .get()
            .then(qSnapshot => {
                const firestore = new Firestore();
                const redis = require("redis");
                const rds = redis
                    .createClient({
                        'host':CONF.redis_host
                    })
                    .on("error", function (err) {
                        console.error(err);
                        callback();
                    });

                var paidTranFSPaths = [];
                console.log("updating the fields of the paid records in Firestore");

                //-- fill the fields with payment acquirer response data which stores in Redis
                //   QuerySnapshot, see https://googleapis.dev/nodejs/firestore/latest/QuerySnapshot.html
                let promises = qSnapshot.docs.map(async (qDocSnapshot) => {
                    let t = qDocSnapshot.data();
                        return new Promise((resolve, reject) => {
                            let id = t.index_ooid;
                            rds.get(id, (err, resp) => {                                
                                if (err) {
                                    reject(err);
                                }
                                else { 
                                    let tranRdsObjs = JSON.parse(resp);
                                    resolve(tranRdsObjs);
                                }
                            });
                        })
                        .then(tranRdsObjs => {
                            if (tranRdsObjs && 'unima' in tranRdsObjs && 0 == tranRdsObjs.unima.status) {
                                let docPath = CONF.fs_collection + '/' + qDocSnapshot.id;
                                paidTranFSPaths.push(docPath);

                                const doc = firestore.doc(docPath);
                                return doc.update({
                                   authorization_code: tranRdsObjs.unima.auth_code,
                                   deposit_date:       new Date(tranRdsObjs.unima.resp_receive_dt)
                                });
                            }
                        })
                        .catch(err => {
                            console.error(err);
                        });
                });

                console.log("getting all transactions which have paid and store info in Redis");

                //-- promise.allsettled, https://www.npmjs.com/package/promise.allsettled
                let allSettled = require('promise.allsettled');
                return allSettled(promises)
                    .then(() => {
                        rds.quit();
                        return paidTranFSPaths;
                    });
            });
///            .then(paidTranFSPaths => {
///                return paidTranFSPaths;
///            });
    });

    let paidTrans = paidTranFSPaths_prom.then(paidTranFSPaths => {
//        console.log(paidTranFSPaths);
        const firestore = new Firestore();
        
        let tran_proms = paidTranFSPaths.map(async (docPath) => {
            //-- get docs according $documentPath
            //   firestore.doc(),  https://googleapis.dev/nodejs/firestore/latest/Firestore.html#doc
            //   DocumentReference, https://googleapis.dev/nodejs/firestore/latest/DocumentReference.html
            //   DocumentSnapshot, https://googleapis.dev/nodejs/firestore/latest/DocumentReference.html#get
            return firestore.doc(docPath)
                .get()
                .then(docSnapshot_prom => docSnapshot_prom)
                .then(docSnapshot => {
                    let tran = docSnapshot.data();
                    //-- format field $deposit_date
                    let dt_str = moment.unix(tran.deposit_date.seconds).format('YYYY-MM-DD HH:mm:ss');
                    tran.deposit_date = dt_str;
                    return tran;
                });
        });

        let allSettled = require('promise.allsettled');
        return allSettled(tran_proms)
            .then(tran_proms => tran_proms.map(p => p.value));
    });
    
    paidTrans.then(async (trans) => {
        id2title_pairs = [];
        csvHeaders.forEach((item, idx, ary) => {
            id2title_pairs.push({id: item, title: item});
        });
        
        const path = require('path');
        let fileName = path.basename(pathFileName);
        let fNames = fileName.split('.');
        fNames = [fNames[0], 'filtered', 'filled', fNames[1]];
        fileName = fNames.join('.');
        const localPath = CONF.PATH_LOCAL_OUTPUT + fileName;
       
        //-- output the result
        //   see https://www.npmjs.com/package/csv-writer
        const createCsvStringifier = require('csv-writer').createObjectCsvStringifier;
        const csvStringifier = createCsvStringifier({
///            path: localPath,
            header: id2title_pairs 
        });
///        await csvWriter.writeRecords(trans);
        csvTrans_header_str  = csvStringifier.getHeaderString();
        csvTrans_records_str = csvStringifier.stringifyRecords(trans);
//        console.log(csvTrans_header_str);
//        console.log(csvTrans_records_str);

        console.log('write to local csv done.');

        const storage = new Storage();
        return storage
            .bucket(bucketName)
            .file(CONF.PATH_GCS_OUTPUT + fileName)
            .save('\ufeff' + csvTrans_header_str + csvTrans_records_str)
            .catch(err => {
                console.error(err);
            });
         

///        return storage.bucket(bucketName)
///            .upload(localPath, {
///                destination: CONF.PATH_GCS_OUTPUT + fileName 
///            })
///            .then(rs => console.log(`upload to gs://${rs[1].bucket}/${rs[1].name} done.`))
///            .catch(err => console.error(err));

    });


console.log('main thread has run to end'); 


//
//  get date from filename, e.g. Transactions_forEC_20191230.csv
//
function filename2dateStr(fname) {
    let yyyymmdd = fname.match(/_\d+\.csv/g)[0].split('_')[1].split('.csv')[0];
    let dt_str = moment(yyyymmdd + ' 00:00:00', 'YYYYMMDD HH:mm:ss');
    dt_str = dt_str.format('YYYY-MM-DD HH:mm:ss');

    return dt_str;
}


