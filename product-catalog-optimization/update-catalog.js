const fs = require("fs");
const { MongoClient } = require("mongodb");
const es = require("event-stream");

const { memory } = require("./helpers/memory");
const { timing } = require("./helpers/timing");
const { Metrics } = require("./helpers/metrics");
const { Product } = require("./product");

const MONGO_URL = "mongodb://localhost:27017/test-product-catalog";
const catalogUpdateFile = "updated-catalog.csv";

async function main() {
  const mongoClient = new MongoClient(MONGO_URL);
  const connection = await mongoClient.connect();
  const db = connection.db();
  await memory("Update dataset", () =>
    timing("Update dataset", () => updateDataset(db))
  );
}

async function updateDataset(db) {
  const metrics = Metrics.zero();
  const csvContent = fs.readFileSync(catalogUpdateFile, "utf-8");
  const rowsWithHeader = csvContent.split("\n");
  const dataRows = rowsWithHeader.slice(1);

  const bulkUpdateOperations = [];
  const bulkDeleteOperations = [];

  for (let i = 0; i < dataRows.length; i++) {
    try {
      const line = dataRows[i];
      const product = Product.fromCsv(line);

      const existingProduct = await db
        .collection("Products")
        .findOne({ _id: product._id });

      if (existingProduct) {
        // Update existing product
        const updateResult = await db
          .collection("Products")
          .updateOne({ _id: product._id }, { $set: product });
        updateMetrics(metrics, updateResult, null);
        bulkUpdateOperations.push({
          updateOne: {
            filter: { _id: product._id },
            update: { $set: product },
          },
        });
      } else {
        // Insert new product
        const insertResult = await db.collection("Products").insertOne(product);
        updateMetrics(metrics, insertResult, null);
      }

      // Log progress
      logProgress(i, dataRows.length);
    } catch (error) {
      console.error(`[ERROR] Error processing line ${i}:`, error);
    }
  }

  await executeBulkOperations(
    db,
    metrics,
    bulkUpdateOperations,
    bulkDeleteOperations
  );
  logMetrics(dataRows.length, metrics);
}

async function executeBulkOperations(
  db,
  metrics,
  bulkUpdateOperations,
  bulkDeleteOperations
) {
  try {
    const updateResult = await db
      .collection("Products")
      .bulkWrite(bulkUpdateOperations);
    bulkUpdateOperations.length = 0; // Clear the array after execution

    const dbIds = (
      await db.collection("Products").find({}, { _id: 1 }).toArray()
    ).map((o) => o._id);

    // Add bulk delete operations for products that were not updated or added
    bulkDeleteOperations.push(
      ...dbIds
        .filter(
          (id) =>
            !bulkUpdateOperations.some((op) =>
              op.updateOne.filter._id.equals(id)
            )
        )
        .map((id) => ({
          deleteOne: {
            filter: { _id: id },
          },
        }))
    );

    // Execute bulk delete operations only if there are products to delete
    if (bulkDeleteOperations.length > 0) {
      const deleteResult = await db
        .collection("Products")
        .bulkWrite(bulkDeleteOperations);
      bulkDeleteOperations.length = 0; // Clear the array after execution
      updateMetrics(metrics, updateResult, deleteResult);
    } else {
      console.debug("[DEBUG] No products to delete.");
      updateMetrics(metrics, updateResult, null);
    }
  } catch (error) {
    console.error("[ERROR] Error executing bulk operations:", error);
  }
}

function updateMetrics(metrics, updateResult, deleteResult) {
  if (updateResult.modifiedCount) {
    metrics.updatedCount += updateResult.modifiedCount;
  }
  if (updateResult.upsertedCount) {
    metrics.addedCount += updateResult.upsertedCount;
  }
  if (deleteResult && deleteResult.deletedCount) {
    metrics.deletedCount += deleteResult.deletedCount;
  }
}

function logProgress(nbCsvRows, closestPowOf10) {
  const progressIndicator = (nbCsvRows * 100) / closestPowOf10;
  if (progressIndicator % 10 === 0) {
    console.debug(`[DEBUG] Processed ${nbCsvRows} rows...`);
  }
}

function logMetrics(numberOfProcessedRows, metrics) {
  console.info(`[INFO] Processed ${numberOfProcessedRows} CSV rows.`);
  console.info(`[INFO] Added ${metrics.addedCount} new products.`);
  console.info(`[INFO] Updated ${metrics.updatedCount} existing products.`);
  console.info(`[INFO] Deleted ${metrics.deletedCount} products.`);
}

if (require.main === module) {
  main()
    .then(() => {
      console.log("SUCCESS");
      process.exit(0);
    })
    .catch((err) => {
      console.log("FAIL");
      console.error(err);
      process.exit(1);
    });
}
