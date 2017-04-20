const config = require('config');

const PROJECT_ID = config.get("projectId");
const TOPIC_NAME = config.get("topicName");
const DATASET = config.get("dataset");
const TABLE = config.get("table");
const INSERT_INTERVAL = config.get("insertInterval");

const pubsub = require('@google-cloud/pubsub')({
  projectId: PROJECT_ID
});
const bigquery = require('@google-cloud/bigquery')({
  projectId: PROJECT_ID
});
const winston = require("winston");
const logger = new winston.Logger({
  transports: [
    new (winston.transports.Console)({
      timestamp: function() {
        return Date.now();
      },
      formatter: function(options) {
        // Return string will be passed to logger.
        return (new Date(options.timestamp()).toISOString()) +' '+ options.level.toUpperCase() +' '+ (options.message ? options.message : '') +
          (options.meta && Object.keys(options.meta).length ? '\n\t'+ JSON.stringify(options.meta) : '' );
      }
    })
  ]
});

const topic = pubsub.topic(TOPIC_NAME);
winston.info("Reading from Pub/Sub topic " + TOPIC_NAME);

const wikiDs = bigquery.dataset(DATASET);
const editT = wikiDs.table(TABLE);
winston.info("Writing to BQ Table " + PROJECT_ID + ":" + DATASET + ":" + TABLE);

winston.info("Insert interval is " + (INSERT_INTERVAL / 1000) + "s");

topic.subscribe((err, subscription, apiRes) => {
  if(err) {
    logger.error(err);
  } else {
    let batch = [];
    subscription.on("message", (msg) => {
      if(msg) {
        batch.push(msg.data);
      }
    });
    setInterval(() => {
      if(batch.length === 0) {
        logger.info("Got no events!");
        return;
      }
      logger.info("Inserting " + batch.length + " items into BigQuery!");
      editT.insert(batch, (err, api) => {
        if(err) {
          logger.error(err);
          if(err.errors) {
            logger.error(err.errors);
          }
        }
      });
      batch = [];
    }, INSERT_INTERVAL);
  }
});
