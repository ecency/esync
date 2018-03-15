const steem = require('steem');
const _ = require('lodash');
const http = require('http');
const https = require('https');

const utils = require('./helpers/utils');
const mongoose = require('mongoose');
const Regex = require("regex");
const config = require('./config.js');
let {options} = config;


http.globalAgent.maxSockets = 100;
https.globalAgent.maxSockets = 100;

if (process.env.STEEMJS_URL) {
  steem.api.setOptions({ url: process.env.STEEMJS_URL });
} else {
  steem.api.setOptions({ url: options.url });
}


//function clearGC() {
//  try {
//    global.gc();
//  } catch (e) {
//    console.log("You must run program with 'node --expose-gc index.js' or 'npm start'");
//  }
//}

//setInterval(clearGC, 6000000); //10 hr

//var schedule = require('node-schedule');

//var jj = schedule.scheduleJob('0 0 */8 * *', function(){
//  console.log('Restarting script');
//  process.exit();
//});

mongoose.connect(options.db_url);

// define model =================
let sdb_votes = mongoose.model('sdb_votes', {
    _id: String,
    voter: String,
    weight: String,
    author: String,
    permlink: String,
    timestamp: { type: Date, expires: '90d'},
  });
let sdb_transfers = mongoose.model('sdb_transfers', {
    _id: String,
    from: String,
    to: String,
    amount: String,
    memo: String,
    timestamp: { type: Date, expires: '90d'},
  });
let sdb_follows = mongoose.model('sdb_follows', {
    _id: String,
    follower: String,
    following: String,
    blog: Boolean,
    timestamp: { type: Date, expires: '90d'},
  });
let sdb_reblogs = mongoose.model('sdb_reblogs', {
    _id: String,
    account: String,
    author: String,
    permlink: String,
    timestamp: { type: Date, expires: '90d'},
  });
let sdb_claim_reward_balances = mongoose.model('sdb_claim_reward_balances', {
    _id: String,
    account: String,
    reward_steem: String,
    reward_sbd: String,
    reward_vests: String,
    timestamp: { type: Date, expires: '90d'},
  });
let sdb_comments = mongoose.model('sdb_comments', {
    _id: String,
    parent_author: String,
    parent_permlink: String,
    author: String,
    permlink: String,
    title: String,
    body: { type: String, default: ''},
    json_metadata: String,
    timestamp: { type: Date, expires: '90d'},
  });
let sdb_mentions = mongoose.model('sdb_mentions', {
    _id: String,
    account: String,
    post: Boolean,
    author: String,
    permlink: String,
    timestamp: { type: Date, expires: '90d'},
  });
let sdb_comment_options = mongoose.model('sdb_comment_options', {
    _id: String,
    author: String,
    permlink: String,
    max_accepted_payout: String,
    percent_steem_dollars: String,
    allow_votes: Boolean,
    allow_curation_rewards: Boolean,
    extensions: String,
    timestamp: { type: Date, expires: '90d'},
  });
let sdb_account_updates = mongoose.model('sdb_account_updates', {
    _id: String,
    account: String,
    posting: String,
    active: String,
    owner: String,
    memo_key: String,
    json_metadata: String,
    timestamp: { type: Date, expires: '90d'},
  });
let sdb_producer_rewards = mongoose.model('sdb_producer_rewards', {
    _id: String,
    producer: String,
    vesting_shares: String,
    timestamp: { type: Date, expires: '90d'},
  });
let sdb_curation_rewards = mongoose.model('sdb_curation_rewards', {
    _id: String,
    curator: String,
    reward: String,
    comment_author: String,
    comment_permlink: String,
    timestamp: { type: Date, expires: '90d'},
  });
let sdb_author_rewards = mongoose.model('sdb_author_rewards', {
    _id: String,
    author: String,
    permlink: String,
    sbd_payout: String,
    steem_payout: String,
    vesting_payout: String,
    timestamp: { type: Date, expires: '90d'},
  });
let sdb_delegate_vesting_shares = mongoose.model('sdb_delegate_vesting_shares', {
    _id: String,
    delegator: String,
    delegatee: String,
    vesting_shares: String,
    timestamp: { type: Date, expires: '90d'},
  });
let sdb_comment_benefactor_rewards = mongoose.model('sdb_comment_benefactor_rewards', {
    _id: String,
    benefactor: String,
    author: String,
    permlink: String,
    reward: String,
    timestamp: { type: Date, expires: '90d'},
  });
let sdb_transfer_to_vestings = mongoose.model('sdb_transfer_to_vestings', {
    _id: String,
    from: String,
    to: String,
    amount: String,
    timestamp: { type: Date, expires: '90d'},
  });
let sdb_fill_orders = mongoose.model('sdb_fill_orders', {
    _id: String,
    current_owner: String,
    current_orderid: String,
    current_pays: String,
    open_owner: String,
    open_orderid: String,
    open_pays: String,
    timestamp: { type: Date, expires: '90d'},
  });
let sdb_return_vesting_delegations = mongoose.model('sdb_return_vesting_delegations', {
    _id: String,
    account: String,
    vesting_shares: String,
    timestamp: { type: Date, expires: '90d'},
  });
let sdb_limit_order_creates = mongoose.model('sdb_limit_order_creates', {
    _id: String,
    owner: String,
    orderid: String,
    amount_to_sell: String,
    min_to_receive: String,
    fill_or_kill: Boolean,
    expiration: Date,
    timestamp: { type: Date, expires: '90d'},
});
let sdb_withdraw_vestings = mongoose.model('sdb_withdraw_vestings', {
    _id: String,
    account: String,
    vesting_shares: String,
    timestamp: { type: Date, expires: '90d'},
});
let sdb_account_witness_votes = mongoose.model('sdb_account_witness_votes', {
    _id: String,
    account: String,
    witness: String,
    approve: Boolean,
    timestamp: { type: Date, expires: '90d'},
});
let sdb_fill_vesting_withdraws = mongoose.model('sdb_fill_vesting_withdraws', {
    _id: String,
    from_account: String,
    to_account: String,
    withdrawn: String,
    deposited: String,
    timestamp: { type: Date, expires: '90d'},
});
let sdb_states = mongoose.model('sdb_states', {
    _id: String,
    blockNumber: String,
    timestamp: Date
});

//===================

let awaitingBlocks = [];

function getBlockNum() {
  return new Promise ((resolve, reject) => {
    sdb_states.find({}, function(err,res){
      if (err) {
        console.log(err);
        resolve(undefined);
      }
      else {
        console.log(res);
        var b = res[0].blockNumber || options.startingBlock || 0;
        resolve(b);
      }
    });
  });
}

const start = async () => {
  let started; 
  
  const lastBlockNum = await getBlockNum();
  console.log('Last Block Num', lastBlockNum);
  //876000 blocks ~ 1 month
  //2628000 blacks ~ 3 month
  utils.streamBlockNumFrom(lastBlockNum, options.delayBlocks, async (err, blockNum) => {
    awaitingBlocks.push(blockNum);

    if (!started) {
      started = true;
      await parseNextBlock();
    }
  });
};

const numDaysBetween = function(d1, d2) {
  var diff = Math.abs(d1.getTime() - d2.getTime());
  return diff / (1000 * 60 * 60 * 24 * 90);
};

function getBlockAsync(blockNum, virtual) {
  return new Promise ((resolve, reject) => {
    steem.api.getOpsInBlock(blockNum, virtual, (err, res) => {
      if (err) {
        console.log(res);
        resolve([]);
      }
      else {
        resolve(res);
      }
    });
  });
}

function onlyUnique(value, index, self) { 
  return self.indexOf(value) === index;
}
function safelyParseJSON (json) {
  var parsed

  try {
    parsed = JSON.parse(json)
  } catch (e) {
    // Oh well, but whatever...
  }

  return parsed // Could be undefined!
}


const parseNextBlock = async () => {
  if (awaitingBlocks[0]) {
    const blockNum = awaitingBlocks[0];

    /** Parse Block And Do Vote */
    const block = await getBlockAsync(blockNum, false)
    //const block = await steem.api.getBlockWithAsync({ blockNum });
    let blockTime = new Date();
    if (block.length>0) {

      let votes=[],transfers=[],follows=[],reblogs=[],rewards=[],mentions=[],
          comments=[],comment_options=[],account_updates=[],producer_rewards=[],
          curation_rewards=[],author_rewards=[],delegate_vesting_shares=[],comment_benefactor_rewards=[],
          transfer_to_vestings=[],fill_orders=[],return_vesting_delegations=[],withdraw_vestings=[],
          limit_order_creates=[],fill_vesting_withdraws=[],account_witness_votes=[];

      if (numDaysBetween(new Date(), new Date(block[0].timestamp))<90) {

        for (var i = 0; i < block.length; i++) {

          let op = block[i].op;
          let salt = i;
          let indx = blockNum+'-'+block[i].trx_in_block+'-'+salt;
          let timestamp = new Date(block[i].timestamp);
          blockTime = timestamp;
          op[1].timestamp = timestamp;
          op[1].indx = indx;

          let oop = op[1];

          if (op[0]==='vote') {

            votes.push({
              _id: oop.indx,
              voter: oop.voter,
              weight: oop.weight,
              author: oop.author,
              permlink: oop.permlink,
              timestamp: oop.timestamp
            });
          }
          if (op[0]==='transfer') {
            transfers.push({
              _id: oop.indx,
              from: oop.from,
              to: oop.to,
              amount: oop.amount,
              memo: oop.memo,
              timestamp: oop.timestamp
            });
          }
          if (op[0]==='custom_json') {
            let json;

            if (oop.id==='follow') {
              
              json = safelyParseJSON(oop.json);
              
              if (json && json[0]==='follow') {
                if (json[1].what.length>0) {
                  json[1].blog = true;
                } else {
                  json[1].blog = false;
                }
                follows.push({
                  _id: oop.indx,
                  follower: json[1].follower,
                  following: json[1].following,
                  blog: json[1].blog,
                  timestamp: oop.timestamp
                });
              } else if (json && json[0]==='reblog') {
                reblogs.push({
                  _id: oop.indx,
                  account: json[1].account,
                  author: json[1].author,
                  permlink: json[1].permlink,
                  timestamp: oop.timestamp
                });
              }
            }
          }
          if (op[0]==='claim_reward_balance') {
            rewards.push({
              _id: oop.indx,
              account: oop.account,
              reward_steem: oop.reward_steem,
              reward_sbd: oop.reward_sbd,
              reward_vests: oop.reward_vests,
              timestamp: oop.timestamp
            });
          }
          if (op[0]==='comment') {
            let regg = /(?:^|[^a-zA-Z0-9_＠\/!@#$%&*.])(?:(?:@|＠)(?!\/))([a-zA-Z0-9-.]{3,16})(?:\b(?!@|＠)|$)/g;
            
            if (oop.body && oop.body.indexOf('@@')===-1) {

              let lmentions = oop.body.match(regg);
              let postType = false;
              let mm = [];

              oop.parent_author === ''?postType=true:postType=false;

              if (lmentions && lmentions.length>0) {
                //console.log('mentions',mentions);
                for (var io = 0; io < lmentions.length; io++) {
                  var tm = lmentions[io].split('@')[1];
                  if (tm !== oop.author) {
                    if (isNaN(parseInt(tm))) {
                      mm.push(tm);
                    }
                  }
                }
                //console.log(mm);
                let mn = mm.filter((el, k, a) => k === a.indexOf(el));
                for (var j = 0; j < mn.length; j++) {
                  mentions.push({
                    _id: oop.indx+'-'+j,
                    author: oop.author,
                    permlink: oop.permlink,
                    post: postType,
                    account: mn[j],
                    timestamp: oop.timestamp
                  });
                }
              }
            }

            comments.push({
              _id: oop.indx,
              parent_author: oop.parent_author,
              parent_permlink: oop.parent_permlink,
              author: oop.author,
              permlink: oop.permlink,
              title: oop.title,
              body: oop.body,
              json_metadata: oop.json_metadata,
              timestamp: oop.timestamp
            });
          }
          if (op[0]==='comment_options') {
            oop.extensions = JSON.stringify(oop.extensions);
            comment_options.push({
              _id: oop.indx,
              author: oop.author,
              permlink: oop.permlink,
              max_accepted_payout: oop.max_accepted_payout,
              allow_votes: oop.allow_votes,
              allow_curation_rewards: oop.allow_curation_rewards,
              extensions: oop.extensions,
              timestamp: oop.timestamp
            });
          }
          if (op[0]==='account_update') {
            oop.active = JSON.stringify(oop.active);
            oop.posting = JSON.stringify(oop.posting);
            oop.owner = JSON.stringify(oop.owner);
            account_updates.push({
              _id: oop.indx,
              account: oop.account,
              posting: oop.posting,
              active: oop.active,
              owner: oop.owner,
              memo_key: oop.memo_key,
              json_metadata: oop.json_metadata,
              timestamp: oop.timestamp
            });
          }
          if (op[0]==='producer_reward') {
            producer_rewards.push({
              _id: oop.indx,
              producer: oop.producer,
              vesting_shares: oop.vesting_shares,
              timestamp: oop.timestamp
            });
          }
          if (op[0]==='curation_reward') {
            curation_rewards.push({
              _id: oop.indx,
              curator: oop.curator,
              reward: oop.reward,
              comment_author: oop.comment_author,
              comment_permlink: oop.comment_permlink,
              timestamp: oop.timestamp
            });
          }
          if (op[0]==='author_reward') {
            author_rewards.push({
              _id: oop.indx,
              author: oop.author,
              permlink: oop.permlink,
              sbd_payout: oop.sbd_payout,
              steem_payout: oop.steem_payout,
              vesting_payout: oop.vesting_payout,
              timestamp: oop.timestamp
            });
          }
          if (op[0]==='delegate_vesting_shares') {
            delegate_vesting_shares.push({
              _id: oop.indx,
              delegator: oop.delegator,
              delegatee: oop.delegatee,
              vesting_shares: oop.vesting_shares,
              timestamp: oop.timestamp
            });
          }
          if (op[0]==='comment_benefactor_reward') {
            comment_benefactor_rewards.push({
              _id: oop.indx,
              benefactor: oop.benefactor,
              author: oop.author,
              permlink: oop.permlink,
              reward: oop.reward,
              timestamp: oop.timestamp
            });
          }
          if (op[0]==='transfer_to_vesting') {
            transfer_to_vestings.push({
              _id: oop.indx,
              from: oop.from,
              to: oop.to,
              amount: oop.amount,
              timestamp: oop.timestamp
            });
          }
          if (op[0]==='fill_order') {
            fill_orders.push({
              _id: oop.indx,
              current_owner: oop.current_owner,
              current_orderid: oop.current_orderid,
              current_pays: oop.current_pays,
              open_owner: oop.open_owner,
              open_orderid: oop.open_orderid,
              open_pays: oop.open_pays,
              timestamp: oop.timestamp
            });
          }
          if (op[0]==='return_vesting_delegation') {
            return_vesting_delegations.push({
              _id: oop.indx,
              account: oop.account,
              vesting_shares: oop.vesting_shares,
              timestamp: oop.timestamp
            });
          }
          if (op[0]==='limit_order_create'){
            limit_order_creates.push({
              _id: oop.indx,
              owner: oop.owner,
              orderid: oop.orderid,
              amount_to_sell: oop.amount_to_sell,
              min_to_receive: oop.min_to_receive,
              fill_or_kill: oop.fill_or_kill,
              expiration: oop.expiration,
              timestamp: oop.timestamp
            });
          }
          if (op[0]==='withdraw_vesting'){
            withdraw_vestings.push({
              _id: oop.indx,
              account: oop.account,
              vesting_shares: oop.vesting_shares,
              timestamp: oop.timestamp
            });
          }
          if (op[0]==='account_witness_vote') {
            
            account_witness_votes.push({
              _id: oop.indx,
              account: oop.account,
              witness: oop.witness,
              approve: oop.approve,
              timestamp: oop.timestamp
            });
          }
          if (op[0]==='fill_vesting_withdraw') {
            fill_vesting_withdraws.push({
              _id: oop.indx,
              from_account: oop.from_account,
              to_account: oop.to_account,
              withdrawn: oop.withdrawn,
              deposited: oop.deposited,
              timestamp: oop.timestamp
            });
          }
        }//for

        if (votes.length>0) {
          sdb_votes.collection.insert(votes, function(err,res){
            if (err){
              console.log('votes',err);
            }
          });
        }
        if (transfers.length>0) {
          sdb_transfers.collection.insert(transfers, function(err,res){
            if (err){
              console.log('transfers',err);
            }
          });
        }
        if (follows.length>0) {
          sdb_follows.collection.insert(follows, function(err,res){
            if (err){
              console.log('follows',err);
            }
          });
        }
        if (reblogs.length>0) {
          sdb_reblogs.collection.insert(reblogs, function(err,res){
            if (err){
              console.log('reblogs',err);
            }
          });
        }
        if (mentions.length>0) {
          sdb_mentions.collection.insert(mentions, function(err,res){
            if (err){
              console.log('mentions',err);
            }
          });
        }
        if (comments.length>0) {
          sdb_comments.collection.insert(comments, function(err,res){
            if (err){
              console.log('comments',err);
            }
          });
        }
        if (comment_options.length>0) {
          sdb_comment_options.collection.insert(comment_options, function(err,res){
            if (err){
              console.log('comment_options',err);
            }
          });
        }
        if (rewards.length>0) {
          sdb_claim_reward_balances.collection.insert(rewards, function(err,res){
            if (err){
              console.log('rewards',err);
            }
          });
        }
        if (account_updates.length>0) {
          sdb_account_updates.collection.insert(account_updates, function(err,res){
            if (err){
              console.log('account_updates',err);
            }
          });
        }
        if (producer_rewards.length>0) {
          sdb_producer_rewards.collection.insert(producer_rewards, function(err,res){
            if (err){
              console.log('producer_rewards',err);
            }
          });
        }
        if (curation_rewards.length>0) {
          sdb_curation_rewards.collection.insert(curation_rewards, function(err,res){
            if (err){
              console.log('curation_rewards',err);
            }
          });
        }
        if (author_rewards.length>0) {
          sdb_author_rewards.collection.insert(author_rewards, function(err,res){
            if (err){
              console.log('author_rewards',err);
            }
          });
        }
        if (delegate_vesting_shares.length>0) {
          sdb_delegate_vesting_shares.collection.insert(delegate_vesting_shares, function(err,res){
            if (err){
              console.log('delegate_vesting_shares',err);
            }
          });
        }
        if (comment_benefactor_rewards.length>0) {
          sdb_comment_benefactor_rewards.collection.insert(comment_benefactor_rewards, function(err,res){
            if (err){
              console.log('comment_benefactor_rewards',err);
            }
          });
        }
        if (transfer_to_vestings.length>0) {
          sdb_transfer_to_vestings.collection.insert(transfer_to_vestings, function(err,res){
            if (err){
              console.log('transfer_to_vestings',err);
            }
          });
        }
        if (fill_orders.length>0) {
          sdb_fill_orders.collection.insert(fill_orders, function(err,res){
            if (err){
              console.log('fill_orders',err);
            }
          });
        }
        if (return_vesting_delegations.length>0) {
          sdb_return_vesting_delegations.collection.insert(return_vesting_delegations, function(err,res){
            if (err){
              console.log('return_vesting_delegations',err);
            }
          });
        }
        if (withdraw_vestings.length>0) {
          sdb_withdraw_vestings.collection.insert(withdraw_vestings, function(err,res){
            if (err){
              console.log('withdraw_vestings',err);
            }
          });
        }
        if (limit_order_creates.length>0) {
          sdb_limit_order_creates.collection.insert(limit_order_creates, function(err,res){
            if (err){
              console.log('limit_order_creates',err);
            }
          });
        }
        if (fill_vesting_withdraws.length>0) {
          sdb_fill_vesting_withdraws.collection.insert(fill_vesting_withdraws, function(err,res){
            if (err){
              console.log('fill_vesting_withdraws',err);
            }
          });
        }
        if (account_witness_votes.length>0) {
          sdb_account_witness_votes.collection.insert(account_witness_votes, function(err,res){
            if (err){
              console.log('account_witness_votes',err);
            }
          });
        }
      }//if numberofDays
    }//if block

    /** Store On DB Last Parsed Block */
    try {
      await sdb_states.updateOne({}, { blockNumber: blockNum, timestamp: blockTime }, { "multi" : false, "upsert" : true });
      console.log('Block Parsed', blockNum);
    } catch (err) {
      console.log('Error Save Redis', blockNum, err);
    }

    delete awaitingBlocks[0];
    awaitingBlocks = _.compact(awaitingBlocks);

    await parseNextBlock();

  } else {
    await utils.sleep(3010);
    await parseNextBlock();
  }
};

start();
