const MongoClient = require("mongodb").MongoClient;
const assert = require("assert");

const circulationRepo = require("./repos/circulationRepo");
const data = require("./circulation.json");

const url = "mongodb://127.0.0.1:27017";
const dbName = "circulation";

async function main() {
  const client = new MongoClient(url, { useUnifiedTopology: true });

  await client.connect();

  try {
    //load data
    const results = await circulationRepo.loadData(data);
    assert.equal(data.length, results.insertedCount);

    //get data
    const getData = await circulationRepo.get();
    assert.equal(data.length, getData.length);

    //filter data by one item
    const filterData = await circulationRepo.get({
      Newspaper: getData[4].Newspaper,
    });
    assert.deepEqual(filterData[0], getData[4]);

    //limit data
    const limitData = await circulationRepo.get({}, 3);
    assert.equal(limitData.length, 3);

    const id = getData[4]._id.toString();
    const byId = await circulationRepo.getById(id);
    assert.deepEqual(byId, getData[4]);

    //add new item
    const newItem = {
      Newspaper: "Youm7",
      "Daily Circulation, 2004": 100,
      "Daily Circulation, 2013": 200,
      "Change in Daily Circulation, 2004-2013": 100,
      "Pulitzer Prize Winners and Finalists, 1990-2003": 0,
      "Pulitzer Prize Winners and Finalists, 2004-2014": 0,
      "Pulitzer Prize Winners and Finalists, 1990-2014": 0,
    };
    const addedItem = await circulationRepo.add(newItem);
    const queryItem = await circulationRepo.getById(addedItem._id);
    assert.deepEqual(queryItem, newItem);

    //update item
    const updateItem = await circulationRepo.update(addedItem._id, {
      Newspaper: "New Youm7",
      "Daily Circulation, 2004": 100,
      "Daily Circulation, 2013": 200,
      "Change in Daily Circulation, 2004-2013": 100,
      "Pulitzer Prize Winners and Finalists, 1990-2003": 0,
      "Pulitzer Prize Winners and Finalists, 2004-2014": 0,
      "Pulitzer Prize Winners and Finalists, 1990-2014": 0,
    });
    const newAddedItemQuery = await circulationRepo.getById(addedItem._id);
    assert.equal(newAddedItemQuery.Newspaper, "New Youm7");

    // delete item
    const removed = await circulationRepo.remove(addedItem._id);
    assert(removed);
   
    //aggregation 
    const avgFinalists = await circulationRepo.averageFinalist();
    console.log(`avgFinlists = ${avgFinalists}`); 

    //average by change
    const averageByChange = await circulationRepo.averageFinalistByChange();
    console.log(averageByChange);
  } catch (error) {
    console.log(`app.js: error ${error} `);
  } finally {
    const admin = client.db(dbName).admin();

    console.log(await admin.listDatabases());
    await client.db(dbName).dropDatabase();

    //   console.log(await admin.serverStatus());
    console.log(await admin.listDatabases());

    client.close();
  }
}

main();
