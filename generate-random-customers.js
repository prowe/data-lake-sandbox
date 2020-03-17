const AWS = require('aws-sdk');
const Chance = require('chance');
const chance = new Chance();

const streamName = process.env.CustomerStreamName;
if (!streamName) {
    console.error('CustomerStreamName env variable not set');
    process.exit(1);
}

function createCustomerEvent() {
    const defaultMax = 1000000;
    const randomNumber = chance.natural({ min: 1, max:  3});
    return {
        id: randomNumber.toString().padStart(6, '0'),
        firstName: chance.first(),
        lastName: chance.last(),
        birthDate: chance.birthday().toJSON().substr(0, 10),
        zipCode: chance.zip(),
        modifiedDate: new Date(Date.now())
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

async function loadCustomers(count) {
    if (isNaN(count)) {
        throw new Error('Not a number!');
    }
    console.log('loading: ', count);

    let remaining = count;
    while (remaining > 0) {
        const batch = chance.n(createCustomerEvent, Math.min(remaining, 500));
        if (count <= 10) {
            console.log(JSON.stringify(batch, null, 2));
        }

        const response = await kinesis.putRecordBatch({
            DeliveryStreamName: streamName,
            Records: batch.map(cust => ({
                Data: JSON.stringify(cust) + '\n'
            }))
        }).promise();

        remaining -= batch.length - response.FailedPutCount;
        console.log(`Attempted: ${batch.length} Failed: ${response.FailedPutCount} Remaining: ${remaining}`);
    }
}

loadCustomers(parseInt(process.argv[2]));