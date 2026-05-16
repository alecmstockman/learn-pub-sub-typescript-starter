import { clientWelcome, getInput, commandStatus, printClientHelp, printQuit } from "../internal/gamelogic/gamelogic.js";
import { SimpleQueueType } from "../internal/pubsub/consume.js";
import { ExchangePerilDirect, ExchangePerilTopic, GameLogSlug, PauseKey, WarRecognitionsPrefix } from "../internal/routing/routing.js"
import amqp from "amqplib";
import { GameState } from "../internal/gamelogic/gamestate.js";
import { commandSpawn } from "../internal/gamelogic/spawn.js";
import { commandMove } from "../internal/gamelogic/move.js";
import { subscribeJSON } from "../internal/pubsub/consume.js";
import { handlerPause } from "./handler.js";
import { handlerMove } from "./handler.js";
import { publishJSON } from "../internal/pubsub/publish.js";
import { ArmyMovesPrefix } from "../internal/routing/routing.js";
import { handlerWar } from "./handler.js";


async function main() {
  const rabbitConnString = "amqp://guest:guest@localhost:5672";
  const conn = await amqp.connect(rabbitConnString);
  const publishCh = await conn.createConfirmChannel();
  console.log("Peril game client connected to RabbitMQ!");
  
  ["SIGINT", "SIGTERM"].forEach((signal) =>
    process.on(signal, async () => {
      try {
        await conn.close();
        console.log("RabbitMQ connection closed.");
      } catch (err) {
        console.error("Error closing RabbitMQ connection:", err);
      } finally {
        process.exit(0);
      }
    }),
  );

  const username = await clientWelcome();
  const gs = new GameState(username);
  
  await subscribeJSON(
    conn, 
    ExchangePerilDirect, 
    `pause.${username}`, 
    PauseKey, 
    SimpleQueueType.Transient,
    handlerPause(gs),
    // JSON.parse(),
  );

  await subscribeJSON(
    conn,
    ExchangePerilTopic,
    `${ArmyMovesPrefix}.${username}`,
    `${ArmyMovesPrefix}.*`,
    SimpleQueueType.Transient,
    handlerMove(gs, publishCh)
  ); 

  await subscribeJSON(
    conn,
    ExchangePerilTopic,
    `${WarRecognitionsPrefix}.${username}`,
    `${WarRecognitionsPrefix}.*`,
    SimpleQueueType.Durable,
    handlerWar(gs, publishCh)
  );

  while (true) {
    const words = await getInput();
    if (words.length === 0) {
      continue;
    }
    const command = words[0];
    if (command === "move") {
      try {
        const armyMove = commandMove(gs, words);
        publishJSON(
          publishCh, 
          ExchangePerilTopic, 
          `${ArmyMovesPrefix}.${username}`, 
          armyMove
        );
      } catch (err) {
        console.log((err as Error).message);
      }
    } else if (command === "spawn") {
      try {
        commandSpawn(gs, words);
      } catch (err) {
        console.log((err as Error).message);
      }
    } else if (command === "status") {
      commandStatus(gs);
    } else if (command === "help") {
      printClientHelp()
    } else if (command === "spam") {
      console.log("Spamming not allowed yet!")
    } else if (command === "quit") {
      printQuit();
      process.exit(0);
    } else {
      console.log("Unknown command");
      continue;
    }
    
  }
}

main().catch((err) => {
  console.error("Fatal error:", err);
  process.exit(1);
});






