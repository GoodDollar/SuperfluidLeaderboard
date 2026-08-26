## How points for airdrop are calculated

Wallet points are sum of:

- number of daily UBI claims on Celo since BLOCK_START
- sqrt of G$s streamed so far to GoodCollective pools.
- valid FLOW council votes from ballots subgraph, capped to 2 counted votes per rolling 2-week window.

> Notice: Max stream rate for score is 73,000 G$ per day

> Notice: Wallet address must be a verified unique human being ie that passed face verification. (You can get verified on: https://gooddapp.org)

### Voting points

- Votes are loaded from FLOW council subgraph ballots by voter id format: ROUND_COUNCIL-address.
- Each counted vote adds VOTE_POINTS.
- At most 2 votes are counted in any rolling 2-week span.

### Campaign IDs

- `STACK_POINT_SYSTEM_ID` is the default campaign for claim/invite/vote events.
- `STACK_G_DONATION_POINT_SYSTEM_ID` is optional and, when set, is used only for G$ donation/streaming events:
  `streamed`, `roundStreamed`, `opensourceStreamed`, `opensourceSent`.

### Campaign API keys

- `STACK_KEY` is the default API key used for claim/invite/vote points API calls.
- `STACK_G_DONATION_KEY` is optional and, when set, is used for G$ donation/streaming points API calls.
- If `STACK_G_DONATION_KEY` is not set, donation/streaming calls fall back to `STACK_KEY`.

### Historical event data source

- `USE_SUBGRAPH` defaults to `false`, which uses the Blockscout PRO API for the historical claim, invite, and direct open-source transfer lookups.
- Set `USE_SUBGRAPH=true` to use the migrated `transferEvents` subgraph lookups for invite and direct open-source transfer actions. Claims always use the UBI `Claimed` event so points follow the event's `claimer`/verified root rather than the G$ payout recipient.
- Stream-period and FLOW council vote calculations remain subgraph-based because they did not have an equivalent legacy Blockscout implementation.
- `BLOCKSCOUT_PRO_API_KEY` is required for the default path and should be stored as a Worker secret (`wrangler secret put BLOCKSCOUT_PRO_API_KEY`). The worker uses Blockscout’s Celo PRO endpoint: `https://api.blockscout.com/v2/api?chain_id=42220`.
- Blockscout `getLogs` is capped at 1,000 logs per request; a full result set is recovered by recursively splitting saturated block ranges rather than trusting page numbers.

### Rate limit

- The worker enforces 1 request per address per hour.
- Window is configurable via `ADDRESS_RATE_LIMIT_SECONDS` (default `3600`).

### Max score

If you claim every day for two years the max points you can get are 730.
If you stream every day 73K G$ for two years the max points you can get are 7300.

### Gas fees

We will subsidize gas fees on base so users can update their points at least once a month.
