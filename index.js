const kafka = require('kafka-node');
const async = require('async');

const INTERVAL = 3 * 1000;
const QUEUE_MAX_LENGTH = 10;
const CONSUMER_GROUP_ID = 'sfa_test_kafka_group_id1';

function Main(cnf, { errors, U, graceful, logger, queue }) {
    const { sleep } = U;
    const kafkaHost = cnf.brokers.join(',')
    const client = new kafka.KafkaClient({
        kafkaHost,
    });
    client.on('ready', () => {
        const topics = Object.values(cnf.topics).filter(t => client.topicMetadata[t]);
        const options = {
            kafkaHost,
            groupId: CONSUMER_GROUP_ID,
            fromOffset: 'latest',
            fetchMaxBytes: 1024 * 1024,
            autoCommit: false,
        };
        const consumer = new kafka.ConsumerGroup(options, topics);
        consumer.on("message", (msg) => {
            logger.info("receive a message by kafka", msg);
            // 记录offset
            consumer.commit(() => {});
            try {
                msg.value = JSON.parse(msg.value);
            } catch (err) {
                logger.error(err, msg.value);
                return;
            }
            queue.push(msg);
        });

        consumer.on("error", (err) => {
            logger.error(errors.sfaKafkaClientError(err));
        });
    });

    const checkQueueLength = async () => {
        if (queue.length() < QUEUE_MAX_LENGTH) return;
    
        consumer.pause();
        await async.whilst(
            async () => QUEUE_MAX_LENGTH <= queue.length(),
            async () => {
                logger.info(`checkQueueLength: ${queue.length()}`);
                await sleep(INTERVAL);
            }
        );
        logger.info("consumer.resume");
        consumer.resume();
    };

    async.forever(async () => {
        await sleep(10 * 1000);
        try {
            await checkQueueLength();
        } catch (e) {
            logger.error(e);
        }
    });
}

Main.Deps = ['logger', 'utils'];

module.exports = Main;
