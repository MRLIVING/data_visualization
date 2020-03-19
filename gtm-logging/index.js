var CONF = {     
    'FS_COLLECTION': 'mrl_gtm_logging_transactions',
}

const {Firestore} = require('@google-cloud/firestore');
const moment = require('moment');


/**
 * Responds to any HTTP request.
 *
 * @param {!express:Request} req HTTP request context.
 * @param {!express:Response} res HTTP response context.
 */
exports.logging = async (req, res) => {
    let msg = req.query.message || req.body.message || 'null';
    console.log(msg);
    
    let resp_msg = 'ok';    

    const firestore = new Firestore();
    let log_dt = new Date();    
    console.log(`logging datetime: ${log_dt}`);
  
    if (msg.transaction) {                
        let tran = msg.transaction;
        tran._log_dt = log_dt;        

        //-- set an new document into firestore
        //   firestore.doc(), https://googleapis.dev/nodejs/firestore/latest/Firestore.html#doc        
        let log_dt_str = moment(log_dt).format('YYYYMMDDTHH:mm:ss');
        const docPath = CONF.FS_COLLECTION + '/' + tran.transactionId + '_' + log_dt_str;
        const doc = firestore.doc(docPath);        
        await doc.set(tran);
    }
    else {
        resp_msg = 'nothing to logging';
    }
    
    res.header('Access-Control-Allow-Origin', '*');
    res.header('Access-Control-Allow-Headers', 'Content-Type');
    res.status(200).send(resp_msg);
};
