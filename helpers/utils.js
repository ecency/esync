const steem = require('steem');

const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));

const streamBlockNumFrom = (from, delay, cb) => {
  let updated;
  
  steem.api.streamBlockNumber('irreversible', (err, blockNum) => {
    const delayedBlockNum = blockNum - delay;
    if (!updated && from !== delayedBlockNum) {
      updated = true;
      for (let i = parseInt(from) + 1; i < delayedBlockNum; i++) {
        cb(null, i);
      }
    }
    cb(null, delayedBlockNum);
  });
};

module.exports = {
  streamBlockNumFrom,
  sleep,
};
