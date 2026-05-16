import amqp from "amqplib";
import { publishJSON } from "../internal/pubsub/publish.js";
import { ArmyMovesPrefix, ExchangeDeadLetter, ExchangePerilDirect, ExchangePerilTopic, GameLogSlug, PauseKey } from "../internal/routing/routing.js";
import { type PlayingState } from "../internal/gamelogic/gamestate.js";
import { getInput, printQuit, printServerHelp } from "../internal/gamelogic/gamelogic.js";
import { declareAndBind, SimpleQueueType } from "../internal/pubsub/consume.js";


async function main() {
  const rabbitConnString = "amqp://guest:guest@localhost:5672";
  const conn = await amqp.connect(rabbitConnString);
  console.log("Peril game server connected to RabbitMQ!");

  ["SIGINT", "SIGTERM"].forEach((signal) => 
    process.on(signal, async () => {
      try {
        await conn.close();
        console.log("RabbitMQ connection closed.");
      } catch (err) {
        console.error("Error closing RabbitMQ connection", err);
      } finally {
        process.exit(0);
      }
    }),
  );

  await declareAndBind(
    conn,
    ExchangePerilTopic,
    "game_logs",
    `${GameLogSlug}.*`,
    SimpleQueueType.Durable,
  );

  const channel = await conn.createConfirmChannel();
  printServerHelp();

  while (true) {
    const words = await getInput();

    const command = words[0];
    if (command === "pause") {
      console.log("Publishing paused game state");
      try {
        await publishJSON(channel, ExchangePerilDirect, PauseKey, {
          isPaused: true,
        });
      } catch (err) {
        console.error("Error publishing pause message", err);
      } 
    } else if (command === "resume") {
      console.log("Publishing resumed game state");
      try {
        await publishJSON(channel, ExchangePerilDirect, PauseKey, {
          isPaused: false,
        });
      } catch (err) {
        console.error("Error publishing pause message", err)
      } 
    } else if (command === "quit") {
      console.log("Goodbye!")
      process.exit(0);

    } else {
      console.log("Unknown command");
    }
  }
}

main().catch((err) => {
  console.error("Fatal error:", err);
  process.exit(1);
});
