# Implementation To-Do

- Retry Raider.IO `run-details` fetches for known `keystone_run_id` records when the run document exists but `detail_loaded` is still false. Right now, if the first detail fetch fails and the same run is later rediscovered through another rostered player, the bot skips the detail fetch because the run ID is already known.
- We're pulling in "alternate runs" data right now. I think that's runs for registered alts of the toon on raider.io, which we don't want. Need to confirm, and strike that if true.
