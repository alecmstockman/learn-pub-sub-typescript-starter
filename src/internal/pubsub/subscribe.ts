import { declareAndBind, type SimpleQueueType } from "./consume.js";
import amqp from "amqplib";


export async function subscribeJSON<T>(
  conn: amqp.ChannelModel,
  exchange: string,
  queueName: string,
  key: string,
  queueType: SimpleQueueType,
  handler: (data: T) => void,
): Promise<void> {

  const [channel, queueInfo] = await declareAndBind(
    conn, 
    exchange, 
    queueName, 
    key, 
    queueType
  );

  await channel.consume(queueInfo.queue, function (msg: amqp.ConsumeMessage | null) {
    if (!msg) return;

    let data: T;
    try {
      data = JSON.parse(msg.content.toString());
    } catch (err) {
      console.error("Could not unmarshal message:", err);
      return;
    }

    handler(data);
    channel.ack(msg);
  });
}