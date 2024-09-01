require('dotenv').config();
const express = require('express')
const { generateSlug } = require('random-word-slugs')
const { ECSClient, RunTaskCommand } = require('@aws-sdk/client-ecs')
const { Server } = require('socket.io')
const { z } = require('zod');
const { PrismaClient } = require('@prisma/client')
const { createClient } = require('@clickhouse/client')
const { Kafka } = require('kafkajs')
const cors = require('cors')
const { v4: uuidv4 } = require('uuid')
const fs = require('fs');
const path = require('path')

const app = express();
app.use(express.json())
app.use(cors())

const PORT = 9000;

const prisma = new PrismaClient({});

const kafka = new Kafka({
    clientId: `api-server`,
    brokers: [process.env.KAFKA_BROKER],
    ssl: {
        ca: [fs.readFileSync(path.join(__dirname, 'kafka.pem'), 'utf-8')]
    },
    sasl: {
        username: process.env.KAFKA_USERNAME,
        password: process.env.KAFKA_PASSWORD,
        mechanism: 'plain'
    }
})

const client = createClient({
    host: process.env.CLICKHOUSE_HOST,
    database: process.env.CLICKHOUSE_DATABASE,
    username: process.env.CLICKHOUSE_USERNAME,
    password: process.env.CLICKHOUSE_PASSWORD
})

const consumer = kafka.consumer({ groupId: 'api-server-logs-consumer' })


// const subscriber = new Redis(`redis://default:${process.env.REDIS_PASSWORD}@${process.env.REDIS_HOST}:${process.env.REDIS_PORT}`)

const ecsClient = new ECSClient({
    region: 'ap-south-1',
    credentials: {
        accessKeyId: process.env.accessKeyId,
        secretAccessKey: process.env.secretAccessKey
    }
})

const config = {
    CLUSTER: '',
    TASK: ''
}


const io = new Server({ cors: '*' });


io.on('connection', socket => {
    console.log('new connection');
    socket.on('subscribe', channel => {
        socket.join(channel)
        socket.emit('message', `Joined ${channel}`)
    })
})


app.post('/project', async (req, res) => {
    const { gitURL, name, slug } = req.body;
    const projectSlug = slug ? slug : generateSlug();

    const schema = z.object({
        name: z.string(),
        gitURL: z.string()
    });

    const safeParseResult = schema.safeParse(req.body);

    if (safeParseResult.error) {
        return res.status(400).json({ error: safeParseResult.error });
    }

    const project = await prisma.project.create({
        data: {
            name, gitURL, subDomain: projectSlug, customDomain: "null"
        }
    });

    return res.status(200).json({ status: 'success', data: { project } });
});


app.get('/logs/:id', async (req, res) => {
    const id = req.params.id;
    const logs = await client.query({
        query: `SELECT event_id, deployment_id, log, timestamp from log_events where deployment_id = {deployment_id:String}`,
        query_params: {
            deployment_id: id
        },
        format: 'JSONEachRow'
    })

    const rawLogs = await logs.json()

    return res.json({ logs: rawLogs })
})


app.post('/deploy', async (req, res) => {
    const { projectId } = req.body;
    if (!projectId) {
        return res.status(400).json({ error: 'projectId required' });
    }
    const project = await prisma.project.findUnique({ where: { id: projectId } });

    if (!project) {
        return res.status(404).json({ error: 'project not found' });
    }

    const deployment = await prisma.deployement.create({
        data: {
            project: { connect: { id: projectId } },
            status: 'QUEUED'
        }
    });

    const command = new RunTaskCommand({
        cluster: config.CLUSTER,
        taskDefinition: config.TASK,
        launchType: 'FARGATE',
        count: 1,
        networkConfiguration: {
            awsvpcConfiguration: {
                assignPublicIp: 'ENABLED',
                subnets: ["", "", ""],
                securityGroups: [""],
            }
        },
        overrides: {
            containerOverrides: [
                {
                    name: 'builder-image',
                    environment: [
                        { name: 'GIT_REPOSITORY__URL', value: project.gitURL },
                        { name: 'PROJECT_ID', value: projectId },
                        { name: "DEPLOYMENT_ID", value: deployment.id }
                    ]
                }
            ]
        }
    })


    await ecsClient.send(command);

    return res.json({ status: 'queued', data: { projectSlug: project.subDomain, url: `http://${projectId}.localhost:8000`, project } })
});

async function initkafkaConsumer() {
    await consumer.connect();
    await consumer.subscribe({ topics: ['container-logs'] });

    await consumer.run({
        autoCommit: false,
        eachBatch: async function ({ batch, heartbeat, commitOffsetsIfNecessary, resolveOffset }) {
            const messages = batch.messages;
            console.log(`Recv. ${messages.length} messages..`)
            for (const message of messages) {
                if (!message.value) continue;
                const stringMessage = message.value.toString();
                const { PROJECT_ID, DEPLOYEMENT_ID, log } = JSON.parse(stringMessage)
                console.log({ log, DEPLOYEMENT_ID });
                try {
                    const { query_id } = await client.insert({
                        table: 'log_events',
                        values: [{ event_id: uuidv4(), deployment_id: DEPLOYEMENT_ID, log }],
                        format: 'JSONEachRow'
                    });
                    console.log(query_id)
                    resolveOffset(message.offset);
                    await commitOffsetsIfNecessary(message.offset);
                    await heartbeat();
                } catch (err) {
                    console.log(err)
                }
            }
        }
    })
}

initkafkaConsumer();

io.listen(9002, () => console.log('Socket Server 9002'))

app.listen(PORT, () => console.log(`API Server Running..${PORT}`))



// docker run -e GIT_REPOSITORY__URL=https://github.com/Tarunagg1/vite-project.git -e PROJECT_ID=tar build-server -d


// docker run -e GIT_REPOSITORY__URL = https://github.com/Tarunagg1/vite-project.git -e PROJECT_ID=tarrr -e DEPLOYEMENT_ID=1 build-server -d