import { clientWelcome, getInput, commandStatus, printClientHelp, printQuit } from "../internal/gamelogic/gamelogic.js";
import { declareAndBind, SimpleQueueType } from "../internal/pubsub/consume.js";
import { ExchangePerilDirect, ExchangePerilTopic, PauseKey } from "../internal/routing/routing.js"
import amqp from "amqplib";
import { GameState } from "../internal/gamelogic/gamestate.js";
import { commandSpawn } from "../internal/gamelogic/spawn.js";
import { commandMove, MoveOutcome } from "../internal/gamelogic/move.js";
import { subscribeJSON } from "../internal/pubsub/subscribe.js";
import { handlerPause } from "./handler.js";
import { handleMove } from "./handler.js";
// import { ArmyMove } from "../internal/gamelogic/gamedata.js";


async function main() {
  const rabbitConnString = "amqp://guest:guest@localhost:5672";
  const conn = await amqp.connect(rabbitConnString);
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
  );


  await subscribeJSON(
    conn,
    ExchangePerilTopic,
    `army_moves.${username}`,
    "army_moves.*",
    SimpleQueueType.Transient,
    handleMove(gs),
  ); 

  while (true) {
    const words = await getInput();

    const command = words[0];
    try {

      if (command === "spawn") {
        commandSpawn(gs, words);
      } else if (command === "move") {
        commandMove(gs, words);
      } else if (command === "status") {
        commandStatus(gs);
      } else if (command === "help") {
        printClientHelp()
      } else if (command === "spam") {
        console.log("Spamming not allowed yet!")
      } else if (command === "quit") {
        printQuit();
      } else {
        console.log("Unknown command");
        continue;
      }

    } catch (err) {
      console.log(err);
    }
    
  }
}

main().catch((err) => {
  console.error("Fatal error:", err);
  process.exit(1);
});
