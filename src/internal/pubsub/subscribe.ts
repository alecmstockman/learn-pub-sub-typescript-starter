import { declareAndBind, type SimpleQueueType } from "./consume.js";
import amqp from "amqplib";

export enum AckType {
  Ack,
  NackRequeue,
  NackDiscard, 
};


export async function subscribeJSON<T>(
  conn: amqp.ChannelModel,
  exchange: string,
  queueName: string,
  key: string,
  queueType: SimpleQueueType,
  handler: (data: T) => AckType,
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

    try {
      const result = handler(data);
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