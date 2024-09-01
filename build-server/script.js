require('dotenv').config();
const { exec } = require('child_process')
const path = require('path')
const fs = require('fs')
const { S3Client, PutObjectCommand } = require('@aws-sdk/client-s3')
const mime = require('mime-types')
const { Kafka } = require('kafkajs')


const s3Client = new S3Client({
    region: 'ap-south-1',
    credentials: {
        accessKeyId: process.env.accessKeyId,
        secretAccessKey: process.env.secretAccessKey
    }
});

const PROJECT_ID = process.env.PROJECT_ID
const DEPLOYEMENT_ID = process.env.DEPLOYEMENT_ID

const kafka = new Kafka({
    clientId: `docker-build-server-${DEPLOYEMENT_ID}`,
    brokers: [process.env.KAFKA_BROKER],
    ssl: {
        ca: [fs.readFileSync(path.join(__dirname, 'kafka.pem'), 'utf-8')]
    },
    sasl: {
        username: process.env.KAFKA_USERNAME,
        password: process.env.KAFKA_PASSWORD,
        mechanism: 'plain'
    }
});

const producer = kafka.producer();

async function publishLog(log) {
    console.log('publishLog called');
    try {
        await producer.send({ topic: `container-logs`, messages: [{ key: 'log', value: JSON.stringify({ PROJECT_ID, DEPLOYEMENT_ID, log }) }] })
    } catch (error) {
        console.log(error);
    }
}

async function init() {
    await producer.connect();
    console.log('executing scrip.tjs');
    publishLog('Build Started...')

    try {
        const outDirPath = path.join(__dirname, 'output');
        const p = exec(`cd ${outDirPath} && npm install && npm run build`);

        p.stdout.on('data', function (data) {
            console.log(data.toString());
            publishLog(data.toString());
        });

        p.stdout.on('error', function (data) {
            console.log('Error', data.toString());
            publishLog(`error: ${data.toString()}`);
        });

        p.on('close', async function () {
            console.log('Build Complete');
            publishLog(`Build Complete`)

            const distFolderPath = path.join(__dirname, 'output', 'dist');
            const distFolderContents = fs.readdirSync(distFolderPath, { recursive: true })

            publishLog(`Starting to upload`);

            for (const file of distFolderContents) {
                const filePath = path.join(distFolderPath, file);
                if (fs.lstatSync(filePath).isDirectory()) continue;

                console.log('uploading', filePath)
                publishLog(`uploading ${file}`);

                const command = new PutObjectCommand({
                    Bucket: 'vercel-clone-outputtarun',
                    Key: `__outputs/${PROJECT_ID}/${file}`,
                    Body: fs.createReadStream(filePath),
                    ContentType: mime.lookup(filePath)
                })
                await s3Client.send(command)

                publishLog(`uploaded ${file}`);
                console.log('uploaded', filePath)
            }
            publishLog(`Done`)
            console.log('Done...');
            process.exit(0);
        });
    } catch (error) {
        console.log(error);
    }
}



init();