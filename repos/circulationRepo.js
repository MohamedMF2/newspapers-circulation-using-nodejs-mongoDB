const { MongoClient, ObjectID } = require("mongodb");

function circulationRepo() {
  const url = "mongodb://127.0.0.1:27017";
  const dbName = "circulation";

  function remove(id) {
    return new Promise(async (resolve, reject) => {
      const client = new MongoClient(url, { useUnifiedTopology: true });

      try {
        await client.connect();
        const db = client.db(dbName);

        const removedItem = await db
          .collection("newspapers")
          .deleteOne({ _id: ObjectID(id) });

        resolve(removedItem.deletedCount === 1);
        client.close();
      } catch (error) {
        reject(error);
      }
    });
  }

  function update(id, newItem) {
    return new Promise(async (resolve, reject) => {
      const client = new MongoClient(url, { useUnifiedTopology: true });
      try {
        await client.connect();
        const db = client.db(dbName);
        const updatedItem = await db
          .collection("newspapers")
          .findOneAndReplace({ _id: ObjectID(id) }, newItem, {
            returnOriginal: false,
          });

        resolve(updatedItem.value);
        client.close();
      } catch (error) {
        reject(error);
      }
    });
  }

  function add(newItem) {
    return new Promise(async (resolve, reject) => {
      const client = new MongoClient(url, { useUnifiedTopology: true });
      try {
        await client.connect();
        const db = client.db(dbName);

        const addedItem = await db.collection("newspapers").insertOne(newItem);
        // console.log(`addedItem = ${addedItem.ops[0]}`);
        resolve(addedItem.ops[0]);
        client.close();
      } catch (error) {
        reject(error);
      }
    });
  }

  function getById(id) {
    return new Promise(async (resolve, reject) => {
      const client = new MongoClient(url, { useUnifiedTopology: true });
      try {
        await client.connect();
        const db = client.db(dbName);

        const item = db.collection("newspapers").findOne({ _id: ObjectID(id) });
        resolve(await item);

        client.close();
      } catch (error) {
        reject(error);
      }
    });
  }

  function get(query, limit) {
    return new Promise(async (resolve, reject) => {
      const client = new MongoClient(url, { useUnifiedTopology: true });
      try {
        await client.connect();
        const db = client.db(dbName);

        let items = db.collection("newspapers").find(query);

        if (limit > 0) {
          items = items.limit(limit);
        }

        resolve(await items.toArray());
        client.close();
      } catch (error) {
        reject(error);
      }
    });
  }

  function loadData(data) {
    return new Promise(async (resolve, reject) => {
      const client = new MongoClient(url, { useUnifiedTopology: true });
      try {
        await client.connect();
        const db = client.db(dbName);

        results = await db.collection("newspapers").insertMany(data);
        resolve(results);
        client.close();
      } catch (error) {
        reject(error);
      }
    });
  }

  //aggragation pipeline

  function averageFinalist() {
    return new Promise(async (resolve, reject) => {
      const client = new MongoClient(url, { useUnifiedTopology: true });
      try {
        await client.connect();
        const db = client.db(dbName);
        const average = await db.collection("newspapers").aggregate([
          {
            $group: {
              _id: null,
              avgFinalists: {
                $avg: "$Pulitzer Prize Winners and Finalists, 1990-2014",
              },
            },
          },
        ]).toArray();
        resolve(average[0].avgFinalists);
        client.close();
      } catch (error) {
        reject(error);
      }
    });
  }

  function averageFinalistByChange() {
    return new Promise(async (resolve, reject) => {
      const client = new MongoClient(url, { useUnifiedTopology: true });
      try {
        await client.connect();
        const db = client.db(dbName);
        const average = await db.collection("newspapers").aggregate([
          {$project:{
            "Newspaper":1,
            "Pulitzer Prize Winners and Finalists, 1990-2014":1,
            "Change in Daily Circulation, 2004-2013":1,
            overallChange:{
              $cond:{if: {$gte:["$Change in Daily Circulation, 2004-2013",0]},then:"positive",else:"negative"}
            }
          }},{
            $group: {
              _id: "$overallChange",
              avgFinalists: {
                $avg: "$Pulitzer Prize Winners and Finalists, 1990-2014",
              },
            },
          },
        ]).toArray();
        resolve(average);
        client.close();
      } catch (error) {
        reject(error);
      }
    });
  }

  return { loadData, get, getById, add, update, remove, averageFinalist,averageFinalistByChange };
}

module.exports = circulationRepo();
