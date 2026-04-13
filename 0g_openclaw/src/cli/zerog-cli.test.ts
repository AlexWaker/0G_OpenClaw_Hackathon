import { Command } from "commander";
import { beforeAll, beforeEach, describe, expect, it, vi } from "vitest";
import { runRegisteredCli } from "../test-utils/command-runner.js";

const zeroGRestoreCommand = vi.fn(async () => undefined);
const zeroGSyncCommand = vi.fn(async () => undefined);

vi.mock("../commands/zerog-restore.js", () => ({
  zeroGRestoreCommand,
}));

vi.mock("../commands/zerog-sync.js", () => ({
  zeroGSyncCommand,
}));

describe("zerog cli", () => {
  let registerZeroGCli: (typeof import("./zerog-cli.js"))["registerZeroGCli"];

  beforeAll(async () => {
    ({ registerZeroGCli } = await import("./zerog-cli.js"));
  });

  beforeEach(() => {
    zeroGRestoreCommand.mockClear();
    zeroGSyncCommand.mockClear();
  });

  it("registers zerog sync and dispatches to the manual sync command", async () => {
    const program = new Command();
    registerZeroGCli(program);

    const zerog = program.commands.find((command) => command.name() === "zerog");
    expect(zerog).toBeTruthy();
    expect(zerog?.commands.find((command) => command.name() === "sync")).toBeTruthy();

    await runRegisteredCli({
      register: registerZeroGCli as (program: Command) => void,
      argv: ["zerog", "sync"],
    });

    expect(zeroGSyncCommand).toHaveBeenCalledTimes(1);
  });

  it("registers zerog restore and dispatches to the manual restore command", async () => {
    const program = new Command();
    registerZeroGCli(program);

    const zerog = program.commands.find((command) => command.name() === "zerog");
    expect(zerog).toBeTruthy();
    expect(zerog?.commands.find((command) => command.name() === "restore")).toBeTruthy();

    await runRegisteredCli({
      register: registerZeroGCli as (program: Command) => void,
      argv: ["zerog", "restore", "--force"],
    });

    expect(zeroGRestoreCommand).toHaveBeenCalledTimes(1);
    expect(zeroGRestoreCommand).toHaveBeenCalledWith(expect.anything(), {}, { force: true });
  });
});
