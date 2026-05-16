import type { ConfirmChannel } from "amqplib";
import { encode } from "@msgpack/msgpack";
import { ExchangePerilTopic, GameLogSlug } from "../routing/routing.js";
import { type GameLog } from "../gamelogic/logs.js";

export async function publishJSON<T>(
    ch: ConfirmChannel,
    exchange: string,
    routingKey: string,
    value: T,
): Promise<void> {
    const val = JSON.stringify(value);
    ch.publish(exchange, routingKey, Buffer.from(val), {contentType: "application/json"});

    await ch.waitForConfirms();
};

export function publishMsgPack<T>(
    ch: ConfirmChannel,
    exchange: string, 
    routingKey: string,
    value: T,
): Promise<void>{
    const body = encode(value);
    return new Promise((resolve, reject) => {
        ch.publish(
            exchange,
            routingKey,
            Buffer.from(body),
            { contentType: "application/x-msgpack"},
            (err) => {
                if (err !== null) {
                    reject(new Error("Message was NACKed by the broker"))
                } else {
                    resolve();
                }
            },
        );
    });
}

export async function publishGameLog(ch: ConfirmChannel, username: string, message: string): Promise<void> {
  const gameLog: GameLog = {
    username: username,
    message: message,
    currentTime: new Date(),
  };

  await publishMsgPack(
    ch, 
    ExchangePerilTopic, 
    `${GameLogSlug}.${username}`,
    gameLog
  )

  return;
}