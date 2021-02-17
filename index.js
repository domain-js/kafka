const kafka = require("kafka-node");

const INTERVAL = 3 * 1000;
const QUEUE_MAX_LENGTH = 10;

function Main(cnf, { kafkaNode, async, errors, U, logger }) {
  const checkQueueLength = async (consumer, queue) => {
    if (queue.length() < (cnf.kafka.queue_max_length || QUEUE_MAX_LENGTH)) return;
    consumer.pause();
    await async.whilst(
      async () => QUEUE_MAX_LENGTH <= queue.length(),
      async () => {
        logger.info(`checkQueueLength: ${queue.length()}`);
        await sleep(cnf.kafka.interval || INTERVAL);
      }
    );
    logger.info("consumer.resume");
    consumer.resume();
  };

  const consumer = ({ topics, kafkaHost, groupId,  }, queue) => {
    const consumerGroupOptions = {
      kafkaHost,
      groupId,
      fromOffset: "latest",
      fetchMaxBytes: 1024 * 1024,
      autoCommit: false,
    };
    const cg = new kafka.ConsumerGroup(consumerGroupOptions, topics);
    cg.on("message", (msg) => {
      logger.info("receive a message by kafka", msg);
      // 记录offset
      cg.commit(() => {});
      try {
        msg.value = JSON.parse(msg.value);
      } catch (err) {
        logger.error(err, msg.value);
        return;
      }
      queue.push(msg);
      async.forever(async () => {
        await sleep(10 * 1000);
        try {
          await checkQueueLength(cg, queue);
        } catch (e) {
          logger.error(e);
        }
      });
    });

    cg.on("error", (err) => {
      logger.error(errors.sfaKafkaClientError(err));
    });
  };

  const producer = ({ kafkaHost, topic, partition }, queue) => {
    const client = new kafka.KafkaClient({ kafkaHost });
    const producer = new kafka.Producer(client);
    const hanlde = async (message) => {
      const payloads = [
        { topic, message, partition, },
      ];
      producer.send(payloads, function (err, data) {
        if (err) logger.error(err, msg.value);
      });
    };
    producer.on("ready", () => {
      queue(tryCatchLog((hanlde, logger.error), 1));
    });
  };
  return { consumer, producer };
}

Main.Deps = ["logger", "utils"];

module.exports = Main;
