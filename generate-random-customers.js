const AWS = require('aws-sdk');
const Chance = require('chance');
const chance = new Chance();

const streamName = process.env.CustomerStreamName;
if (!streamName) {
    console.error('CustomerStreamName env variable not set');
    process.exit(1);
}

function createCustomerEvent() {
    const randomNumber = chance.natural({ min: 1, max: 1000000 });
    return {
        id: randomNumber.toString().padStart(6, '0'),
        firstName: chance.first(),
        lastName: chance.last(),
        birthDate: chance.birthday().toJSON().substr(0, 10),
        zipCode: chance.zip()
    };
}

function throttle(start, delay) {
    const elapsed = Date.now() - start;
    if (elapsed >= delay) {
        return;
    }
    return new Promise((resolve) => setInterval(resolve, delay - elapsed));
}

const kinesis = new AWS.Firehose({
    region: 'us-east-1'
});

async function loadCustomers() {
    for(let i=0; i<100; i++) {
        const start = Date.now();
        const batch = chance.n(createCustomerEvent, 100);
        await kinesis.putRecordBatch({
            DeliveryStreamName: streamName,
            Records: batch.map(cust => ({
                Data: JSON.stringify(cust) + '\n'
            }))
        }).promise();

        console.log('loaded', i);
        await throttle(start, 500);
    }
}

loadCustomers();