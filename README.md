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

### Rate limit

- The worker enforces 1 request per address per hour.
- Window is configurable via `ADDRESS_RATE_LIMIT_SECONDS` (default `3600`).

### Max score

If you claim every day for two years the max points you can get are 730.
If you stream every day 73K G$ for two years the max points you can get are 7300.

### Gas fees

We will subsidize gas fees on base so users can update their points at least once a month.
