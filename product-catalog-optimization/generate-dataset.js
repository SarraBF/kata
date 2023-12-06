const fs = require("fs").promises; // Use fs.promises for asynchronous file writing
const uuidv4 = require("uuid").v4;
const { MongoClient } = require("mongodb");
const minimist = require("minimist");

const { timing } = require("./helpers/timing");
const { memory } = require("./helpers/memory");
const { Metrics } = require("./helpers/metrics");
const { Product } = require("./product");

const DATABASE_NAME = "test-product-catalog";
const MONGO_URL = `mongodb://localhost:27017/${DATABASE_NAME}`;
const catalogUpdateFile = "updated-catalog.csv";

const { size } = minimist(process.argv.slice(2));
if (!size) {
  throw new Error("Missing 'size' parameter");
}

async function main() {
  const mongoClient = new MongoClient(MONGO_URL, {
    useNewUrlParser: true,
    useUnifiedTopology: true,
  });
  const connection = await mongoClient.connect();
  const db = connection.db();

  // For running the script several times without manually cleaning the data
  await clearExistingData(db);

  await memory("Generate dataset", () =>
    timing("Generate dataset", async () => {
      await generateDataset(db, size);
    })
  );

  await mongoClient.close();
}

async function clearExistingData(db) {
  const listDatabaseResult = await db.admin().listDatabases({ nameOnly: 1 });
  if (listDatabaseResult.databases.find((d) => d.name === DATABASE_NAME)) {
    await db.dropDatabase();
  }

  if (await fs.stat(catalogUpdateFile).catch(() => null)) {
    await fs.unlink(catalogUpdateFile);
  }
}

async function generateDataset(db, catalogSize) {
  await writeCsvHeaders();

  const metrics = Metrics.zero();
  const createdAt = new Date();
  const productPromises = [];

  for (let i = 0; i < catalogSize; i++) {
    const product = generateProduct(i, createdAt);

    // Insert in initial dataset
    productPromises.push(db.collection("Products").insertOne(product));

    // Insert in updated dataset (csv) with a tweak
    const updatedProduct = generateUpdate(product, i, catalogSize);
    metrics.merge(await writeProductUpdateToCsv(product, updatedProduct));

    const progressPercentage = (i * 100) / catalogSize;
    if (progressPercentage % 10 === 0) {
      console.debug(`[DEBUG] Processing ${progressPercentage}%...`);
    }
  }

  await Promise.all(productPromises);

  logMetrics(catalogSize, metrics);
}

async function writeCsvHeaders() {
  await fs.appendFile(
    catalogUpdateFile,
    Object.keys(generateProduct(-1, null)).join(",") + "\n"
  );
}

function generateProduct(index, createdAt) {
  return new Product(
    uuidv4(),
    `Product_${index}`,
    generatePrice(),
    createdAt,
    createdAt
  );
}

function generatePrice() {
  return Math.round(Math.random() * 1000 * 100) / 100;
}

const productEvent = {
  pDelete: 10,
  pUpdate: 10,
  pAdd: 20,
};

function generateUpdate(product, index, catalogSize) {
  const rand = Math.random() * 100;
  if (rand < productEvent.pDelete) {
    // Delete product
    return null;
  }
  if (rand < productEvent.pDelete + productEvent.pUpdate) {
    // Update product
    return new Product(
      product._id,
      `Product_${index + catalogSize}`,
      generatePrice(),
      product.createdAt,
      new Date()
    );
  }
  if (rand < productEvent.pDelete + productEvent.pUpdate + productEvent.pAdd) {
    // Add new product
    return generateProduct(index + catalogSize, new Date());
  }

  // Unchanged product
  return product;
}

async function writeProductUpdateToCsv(product, updatedProduct) {
  if (updatedProduct) {
    if (updatedProduct._id === product._id) {
      // Updated product or no modification => add this line
      await fs.appendFile(catalogUpdateFile, updatedProduct.toCsv() + "\n");
      return updatedProduct.updatedAt !== updatedProduct.createdAt
        ? Metrics.updated()
        : Metrics.zero();
    } else {
      // Keep product
      await fs.appendFile(catalogUpdateFile, product.toCsv() + "\n");
      // Add new product
      await fs.appendFile(catalogUpdateFile, updatedProduct.toCsv() + "\n");
      return Metrics.added();
    }
  } else {
    return Metrics.deleted();
  }
}

function logMetrics(catalogSize, metrics) {
  console.info(`[INFO] ${catalogSize} products inserted in DB.`);
  console.info(`[INFO] ${metrics.addedCount} products to be added.`);
  console.info(
    `[INFO] ${metrics.updatedCount} products to be updated ${(
      (metrics.updatedCount * 100) /
      catalogSize
    ).toFixed(2)}%.`
  );
  console.info(
    `[INFO] ${metrics.deletedCount} products to be deleted ${(
      (metrics.deletedCount * 100) /
      catalogSize
    ).toFixed(2)}%.`
  );
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
