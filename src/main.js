const projectId = "tweeting-164909";

const PubSub = require('@google-cloud/pubsub');
const bigquery = require('@google-cloud/bigquery')({
  projectId: projectId
});
const pubsubClient = PubSub({
  projectId: projectId
});


const topicName = 'wiki-changes';
const topic = pubsubClient.topic(topicName);

const wikiDs = bigquery.dataset('wiki_changes');
const editT = wikiDs.table('edits');

topic.subscribe((err, subscription, apiRes) => {
  if(err) {
    console.error(err);
  } else {
    let batch = [];
    subscription.on("message", (msg) => {
      //console.log(msg.data);
      if(msg) {
        batch.push(msg.data);
      }
      /*editT.insert(msg.data, (err, api) => {
        if(err) {
          console.log(e);
          if(e.errors) {
            console.log(e.errors);
            console.log(msg.data);
          }
        }
      });*/
    });
    setInterval(() => {
      if(batch.length === 0) {
        console.log("Got no entries!")
          return;
      }
      console.log("Inserting " + batch.length + " items into BigQuery!");
      editT.insert(batch, (err, api) => {
        if(err) {
          console.log(err);
          if(err.errors) {
            console.log(err.errors);
          }
          console.log(batch);
        }
        batch = [];
      });
    }, 10000);
  }
});
