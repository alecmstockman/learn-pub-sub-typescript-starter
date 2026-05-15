import type { GameState, PlayingState } from "../internal/gamelogic/gamestate.js";
import { MoveOutcome } from "../internal/gamelogic/move.js";
import { handlePause } from "../internal/gamelogic/pause.js"
import { handleMove } from "../internal/gamelogic/move.js";
import { type ArmyMove } from "../internal/gamelogic/gamedata.js";
import { AckType } from "../internal/pubsub/subscribe.js";


export function handlerPause(gs: GameState): (ps: PlayingState) => AckType {
    return (ps: PlayingState) => {
        handlePause(gs, ps);
        process.stdout.write("> ");
        return AckType.Ack;
    };
};

export function handlerMove(gs: GameState): (move: ArmyMove) => AckType {
    return (move: ArmyMove) => {
        const outcome = handleMove(gs, move);
        try {
            if (outcome === MoveOutcome.Safe || outcome === MoveOutcome.MakeWar) {
                return AckType.Ack;
            } else {
                return AckType.NackDiscard;
            }
        } finally {
            process.stdout.write("> ");
        }
    };
}