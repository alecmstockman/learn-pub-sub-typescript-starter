import type { GameState, PlayingState } from "../internal/gamelogic/gamestate.js";
import { MoveOutcome } from "../internal/gamelogic/move.js";
import { handlePause } from "../internal/gamelogic/pause.js"
import { handleMove } from "../internal/gamelogic/move.js";
import { type ArmyMove, type RecognitionOfWar } from "../internal/gamelogic/gamedata.js";
import { AckType } from "../internal/pubsub/consume.js";
import { publishJSON } from "../internal/pubsub/publish.js";
import { ExchangePerilTopic, WarRecognitionsPrefix } from "../internal/routing/routing.js";
import type { ConfirmChannel } from "amqplib";
import { handleWar, WarOutcome } from "../internal/gamelogic/war.js";
import { publishGameLog } from "../internal/pubsub/publish.js";


export function handlerPause(gs: GameState): (ps: PlayingState) => AckType {
    return (ps: PlayingState) => {
        handlePause(gs, ps);
        process.stdout.write("> ");
        return AckType.Ack;
    };
};

export function handlerMove(
    gs: GameState, 
    ch: ConfirmChannel,
): (move: ArmyMove) => Promise<AckType> {
    return async (move: ArmyMove): Promise<AckType> => {

        try {
            const outcome = handleMove(gs, move);
            switch (outcome) {
                case MoveOutcome.Safe:
                    return AckType.Ack;
                case MoveOutcome.SamePlayer:
                    return AckType.Ack;
                case MoveOutcome.MakeWar:
                    const recognition: RecognitionOfWar = {
                        attacker: move.player, 
                        defender: gs.getPlayerSnap(),
                    };

                    try {
                        await publishJSON(
                            ch, 
                            ExchangePerilTopic, 
                            `${WarRecognitionsPrefix}.${gs.getUsername()}`, 
                            recognition,
                        )
                        return AckType.Ack;
                    } catch (err) {
                        console.log("Error publishing war recognition:", err);
                        return AckType.NackRequeue;
                    } 
                default:
                    return AckType.NackDiscard
            };
        } finally {
            process.stdout.write("> ");
        }
    };
}

export function handlerWar(
    gs: GameState,
    ch: ConfirmChannel,
): (war: RecognitionOfWar) => Promise<AckType> {
    return async (war: RecognitionOfWar): Promise<AckType> => {
        console.log("HANDLER WAR:", war);
        try{
            const outcome = handleWar(gs, war);
            const username = gs.getUsername()
            let message: string;
            
            switch (outcome.result) {
                case WarOutcome.NotInvolved: 
                    return AckType.NackDiscard;
                case WarOutcome.NoUnits:
                    return AckType.NackDiscard;
                case WarOutcome.OpponentWon: 
                    message = `${outcome.winner} won a war against ${outcome.loser}`;
                    break;              
                case WarOutcome.YouWon: 
                    message = `${outcome.winner} won a war against ${outcome.loser}`;
                    break;
                case WarOutcome.Draw: 
                    message = `A war between ${outcome.attacker} and ${outcome.defender} resulted in a draw`;            
                    break;
                default:
                    const unreachable: never = outcome;
                    console.log("Unexpected war resolution", unreachable);
                    return AckType.NackDiscard;
            };

            try {
                await publishGameLog(ch, username, message)
                return AckType.Ack;
            } catch (err) {
                console.log(`Unable to publish game log for ${outcome.result}:`, err);
                return AckType.NackRequeue;
            }

        } finally {
            process.stdout.write("> ");
        }
    }
}