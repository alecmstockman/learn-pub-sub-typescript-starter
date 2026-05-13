import amqp from "amqplib";
import { type Channel } from "amqplib";

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

    const queue = await channel.assertQueue(queueName, {durable: durable, autoDelete: autoDelete, exclusive: exclusive});
    await channel.bindQueue(queue.queue, exchange, key);

    return [channel, queue];
};