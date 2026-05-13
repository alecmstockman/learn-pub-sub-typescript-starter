import type { GameState, PlayingState } from "../internal/gamelogic/gamestate.js";
import type { MoveOutcome } from "../internal/gamelogic/move.js";
import { handlePause } from "../internal/gamelogic/pause.js"
import { handlerMove } from "../internal/gamelogic/move.js";
import { type ArmyMove } from "../internal/gamelogic/gamedata.js";


export function handlerPause(gs: GameState): (ps: PlayingState) => void {
    return (ps: PlayingState) => {
        handlePause(gs, ps);
        process.stdout.write("> ");
    };
};

export function handleMove(gs: GameState): (move: ArmyMove) => void {
    return (move: ArmyMove) => {
        handlerMove(gs, move);
        process.stdout.write("> ");
    };
}