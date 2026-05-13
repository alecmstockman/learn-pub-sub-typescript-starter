import type { ConfirmChannel } from "amqplib";

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