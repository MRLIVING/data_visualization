var CONF = { 
  'redis_host': 'ms-payment.mrl.com.tw',
  'fs_collection': 'mrl_magento2_Transactions_forEC',  
  'VALID_INPUT_FILE_PREFIX': 'ec_data_csv/input/Transactions_',
  'PATH_GCS_OUTPUT': 'ec_data_csv/output/',
  'BQ_DATASET': 'mrl_magento',
}

const {Storage} = require('@google-cloud/storage');
const {Firestore} = require('@google-cloud/firestore');
const {BigQuery} = require('@google-cloud/bigquery');
const readline = require('readline');
const csv = require('csv-parser')
const stripBom = require('strip-bom-stream');
const moment = require('moment');
const path = require('path');


/**
 * Triggered from a change to a Cloud Storage bucket.
 *
 * @param {!Object} event Event payload.
 * @param {!Object} context Metadata for the event.
 */
exports.transFilter = (event, context, callback) => {
    console.log(`Event Type: ${context.eventType}`);

    const file = event;
    var bucketName   = file.bucket;
    var pathFileName = file.name;

    console.log(`gets an event of file: ${pathFileName} in bucket gs://${bucketName}`);
  
    //-- filter valid inputs    
    if (! pathFileName.startsWith(CONF.VALID_INPUT_FILE_PREFIX)) {
	    console.log(`an invalid input: ${pathFileName}`);
	    //-- termination, see https://cloud.google.com/functions/docs/writing/background#function_parameters
        callback();
	    return 0;
	}
      
    var csvHeaders = [];
	//-- insert data from CSV file into Firestore line by line
	const l2fs_prom = new Promise((resolve, reject) => {
		let proms = [];

		let _dt_fnSuffix = filename2dateStr(pathFileName);

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
				console.log(`The file is found in GCS: ${'gs://' + bucketName + '/' +  pathFileName}`);
				csvHeaders = headers;
			})
			.on('data', (line) => {
				line._dt_fnSuffix = new Date(_dt_fnSuffix);

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
		//-- wait all Transcations set into Firestore
		//   promise.allsettled, https://github.com/es-shims/Promise.allSettled
		let allSettled = require('promise.allsettled');
		return allSettled(writeResults);
	})
	.catch(err => {
		console.error(err);
	});


	const paidTranFSPaths_prom = l2fs_prom.then(async () => {
		let _dt_fnSuffix = filename2dateStr(pathFileName);
		let dt_beg = moment(_dt_fnSuffix, 'YYYY-MM-DD HH:mm:ss').add(-30, 'days').toDate();
		let dt_end = moment(_dt_fnSuffix, 'YYYY-MM-DD HH:mm:ss').add(1, 'days').toDate();
		console.log(`Gets transactions between [${moment(dt_beg).format()}, ${moment(dt_end).format()})`);

		await new Firestore()
			.collection(CONF.fs_collection)
			.where('_dt_fnSuffix', '>=', dt_beg)
			.where('_dt_fnSuffix', '<',  dt_end)
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

				console.log("Updates the fields of the paid records in Firestore");

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

				console.log("Waits for updates of all paid transactions in Firestore");

				//-- promise.allsettled, https://www.npmjs.com/package/promise.allsettled
				let allSettled = require('promise.allsettled');
				return allSettled(promises)
					.then(() => {
						rds.quit();
					});
			});

		var paidTranFSPaths = [];
		let dt_beg_deposit = moment(_dt_fnSuffix, 'YYYY-MM-DD HH:mm:ss').toDate();
		let dt_end_deposit = moment(_dt_fnSuffix, 'YYYY-MM-DD HH:mm:ss').add(+1, 'days').toDate();
		console.log(`Gets the transactions whose deposit datetime within [${moment(dt_beg_deposit).format()}, ${moment(dt_end_deposit).format()})`);
		await new Firestore()
			.collection(CONF.fs_collection)
			.where('deposit_date', '>=', dt_beg_deposit)
			.where('deposit_date', '<',  dt_end_deposit)
			.get()
			.then(qSnapshot => {
				qSnapshot.docs.map(qDocSnapshot => {
					let docPath = CONF.fs_collection + '/' + qDocSnapshot.id;
					paidTranFSPaths.push(docPath);
				})
			});

//        console.log(paidTranFSPaths);
		return paidTranFSPaths;
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

	let save2gcs = paidTrans.then(async (trans) => {
		id2title_pairs = [];
		csvHeaders.forEach((item, idx, ary) => {
			id2title_pairs.push({id: item, title: item});
		});

		const path = require('path');
		let fileName = path.basename(pathFileName);
		let fNames = fileName.split('.');
		fNames = [fNames[0], 'filtered', 'filled', fNames[1]];
		fileName = fNames.join('.');        

		//-- output the result
		//   see https://www.npmjs.com/package/csv-writer
		const createCsvStringifier = require('csv-writer').createObjectCsvStringifier;
		const csvStringifier = createCsvStringifier({
			header: id2title_pairs 
		});

		csvTrans_header_str  = csvStringifier.getHeaderString();
		csvTrans_records_str = csvStringifier.stringifyRecords(trans);
//        console.log(csvTrans_header_str);
//        console.log(csvTrans_records_str);

		console.log(`Writes the results to GCS: ${'gs://' + bucketName + '/' + CONF.PATH_GCS_OUTPUT + fileName}`);

		const storage = new Storage();
		let save2gcs_prom = storage
			.bucket(bucketName)
			.file(CONF.PATH_GCS_OUTPUT + fileName)
			.save('\ufeff' + csvTrans_header_str + csvTrans_records_str)
			.catch(err => {
				console.error(err);
			});

		return save2gcs_prom;
	});

    let save2bq = save2gcs.then(() => { 
        let insert2bq = paidTrans.then(async (trans) => {
//            console.log(trans);    
            const bq = new BigQuery();
            
            //-- Dataset.exists
            //   https://googleapis.dev/nodejs/bigquery/latest/Dataset.html#exists
            let ds = bq.dataset(CONF.BQ_DATASET);
            ds = (! (await ds.exists())[0]) ? (await bq.createDataset(CONF.BQ_DATASET))[0] : ds;

            //-- Dataset.CreateTable if the table doesn't exists
            //   https://googleapis.dev/nodejs/bigquery/latest/Dataset.html#createTable
            const tbName = path.basename(pathFileName, '.csv');
            let tb = ds.table(tbName);
            let header_str = csvHeaders.join(','); 
            const options = { 
                schema: header_str
            };        
            tb = (! (await tb.exists())[0]) ? (await ds.createTable(tbName, options))[0] : tb; 

            let csvHeader_trans = trans.map(t => {
                let tt = Object.keys(t)
                .filter(k => csvHeaders.includes(k))
                .reduce((obj, k) => {
                    obj[k] = t[k];
                    return obj; 
                }, {});

                return tt;
            }).map(t => {
                let row = {
                    insertId: t.index_ooid + '_' + t.item_code,
                    json: t
                };

                return row;
            });

            //-- inserts rows with insertId
            //   https://googleapis.dev/nodejs/bigquery/latest/Table.html#insert
            let insert_prom = tb.insert(csvHeader_trans, 
                {
                    raw: true
                }
            )
            .then(apiResp => {
                return apiResp[0];
            })
            .catch(err => {
                console.error(err);
            });

            return insert_prom;
        });

        return insert2bq
            .then(() => {
				console.log('done.');
                callback(null, 'Success!');
			});
    });

	console.log('main thread has run to the end');    
    return save2bq;
}

/**
 *  get date from filename, e.g. Transactions_forEC_20191230.csv
 */
function filename2dateStr(fname) {
    let yyyymmdd = fname.match(/_\d+\.csv/g)[0].split('_')[1].split('.csv')[0];
    let dt_str = moment(yyyymmdd + ' 00:00:00', 'YYYYMMDD HH:mm:ss');
    dt_str = dt_str.format('YYYY-MM-DD HH:mm:ss');

    return dt_str;
}

