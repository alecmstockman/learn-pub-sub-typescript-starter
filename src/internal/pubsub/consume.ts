import amqp from "amqplib";
import { type Channel } from "amqplib";
import { ExchangeDeadLetter } from "../routing/routing.js";
import { decode } from "@msgpack/msgpack";

export enum SimpleQueueType {
    Durable,
    Transient,
}

export async function declareAndBind(
  conn: amqp.ChannelModel,
  exchange: string,
  queueName: string,
  key: string,
  queueType: SimpleQueueType,
): Promise<[Channel, amqp.Replies.AssertQueue]> {
    
    const channel = await conn.createChannel();
    
    let durable = false;
    let autoDelete = false;
    let exclusive = false

    if (queueType === SimpleQueueType.Durable) {
        durable = true;
    }
    if (queueType === SimpleQueueType.Transient) {
        autoDelete = true;
        exclusive = true;
    }

    const queue = await channel.assertQueue(
        queueName, {
            durable: durable, 
            autoDelete: autoDelete, 
            exclusive: exclusive,
            arguments: {
                "x-dead-letter-exchange": ExchangeDeadLetter
            },
        }
    );
    await channel.bindQueue(queue.queue, exchange, key);

    return [channel, queue];
};

export enum AckType {
  Ack,
  NackRequeue,
  NackDiscard, 
};

// (data: Buffer) => decode(data) as T

async function subscribe<T>(
  conn: amqp.ChannelModel,
  exchange: string,
  queueName: string,
  key: string,
  queueType: SimpleQueueType,
  handler: (data: T) => Promise<AckType> | AckType,
  deserializer: (data: Buffer) => T,
): Promise<void> {

  const [channel, queueInfo] = await declareAndBind(
    conn, 
    exchange, 
    queueName, 
    key, 
    queueType, 
  );

  await channel.prefetch(1);

  await channel.consume(queueInfo.queue, async (msg: amqp.ConsumeMessage | null) => {
    if (!msg) return;

    let data: T;
    try {
      data = deserializer(msg.content)
    } catch (err) {
      console.error("Could not unmarshal message:", err);
      return;
    }

    try {
      const result = await handler(data);
      switch (result) {
        case AckType.Ack:
          channel.ack(msg);
          console.log("Ack");
          break;
        case AckType.NackDiscard:
          channel.nack(msg, false, false);
          console.log("NackDiscard");
          break;
        case AckType.NackRequeue:
          channel.nack(msg, false, true);
          console.log("NackRequeue");
          break;
        default:
          const unreachable: never = result;
          console.error("Unexpected ack type:", unreachable);
          return;
      }
    } catch (err) {
      console.error("Error handling message:", err);
      channel.nack(msg, false, false);
      return;
    }
  });
}

export async function subscribeJSON<T>(
  conn: amqp.ChannelModel,
  exchange: string,
  queueName: string,
  key: string,
  queueType: SimpleQueueType,
  handler: (data: T) => Promise<AckType> | AckType,
): Promise<void> {

  subscribe(
    conn,
    exchange,
    queueName,
    key,
    queueType,
    handler,
    (data: Buffer) => JSON.parse(data.toString()),
  )

}

export async function subscribeMsgPack<T>(
    conn: amqp.ChannelModel,
    exchange: string,
    queueName: string,
    key: string,
    queueType: SimpleQueueType,
    handler: (data: T) => Promise<AckType> | AckType,
): Promise<void> {

  subscribe(
    conn,
    exchange,
    queueName,
    key,
    queueType,
    handler,
    (data: Buffer) => decode(data) as T,
  )

}