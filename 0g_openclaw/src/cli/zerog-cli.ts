import type { Command } from "commander";
import { zeroGRestoreCommand } from "../commands/zerog-restore.js";
import { zeroGSyncCommand } from "../commands/zerog-sync.js";
import { defaultRuntime } from "../runtime.js";
import { theme } from "../terminal/theme.js";
import { runCommandWithRuntime } from "./cli-utils.js";
import { formatHelpExamples } from "./help-format.js";

export function registerZeroGCli(program: Command) {
  const zerog = program.command("zerog").description("Run 0G-specific account and memory actions");

  zerog
    .command("sync")
    .description("Upload changed session transcripts and memory Markdown files to 0G now")
    .addHelpText(
      "after",
      () =>
        `\n${theme.heading("Examples:")}\n${formatHelpExamples([
          ["openclaw zerog sync", "Upload changed transcript and memory files immediately."],
        ])}`,
    )
    .action(async () => {
      await runCommandWithRuntime(defaultRuntime, async () => {
        await zeroGSyncCommand(defaultRuntime);
      });
    });

  zerog
    .command("restore")
    .description("Download the latest 0G memory manifest for this wallet to local state now")
    .option("-f, --force", "Re-download and overwrite matching local files from remote state")
    .addHelpText(
      "after",
      () =>
        `\n${theme.heading("Examples:")}\n${formatHelpExamples([
          ["openclaw zerog restore", "Restore the latest 0G-backed memory for the current wallet."],
          [
            "openclaw zerog restore --force",
            "Re-download the remote memory state even when local files already exist.",
          ],
        ])}`,
    )
    .action(async (options: { force?: boolean }) => {
      await runCommandWithRuntime(defaultRuntime, async () => {
        await zeroGRestoreCommand(defaultRuntime, {}, { force: options.force === true });
      });
    });
}
