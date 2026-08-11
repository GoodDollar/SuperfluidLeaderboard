/**
 * Welcome to Cloudflare Workers! This is your first worker.
 *
 * - Run `npm run dev` in your terminal to start a development server
 * - Open a browser tab at http://localhost:8787/ to see your worker in action
 * - Run `npm run deploy` to publish your worker
 *
 * Bind resources to your worker in `wrangler.toml`. After adding bindings, a type definition for the
 * `Env` object can be regenerated with `npm run cf-typegen`.
 *
 * Learn more at https://developers.cloudflare.com/workers/
 */

/**
 * Points:
 * 5 per invite
 * 1 per claim
 * sqrt(total streamed to goodcollective max 73K per day)
 * Before starting a new campaign update the variables for starting block and timestamp
 */
import { isArray, first, isFunction, noop, groupBy, last, add } from 'lodash';
import { celo } from 'viem/chains';
import { createPublicClient, getContract, http, padHex, parseAbi, getAddress, formatEther } from 'viem';
import createClient from 'openapi-fetch';
import { paths } from '../points-api';

const MAX_DAILY_STREAM = BigInt(73000 * 1e18); //73k G$
const MAX_STREAM_RATE = MAX_DAILY_STREAM / (24n * 60n * 60n);
const ERC20_TRANSFER_TOPIC = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef';
const ROUND_STREAM_EVENT_NAME = 'roundStreamed';
const OPENSOURCE_STREAM_EVENT_NAME = 'opensourceStreamed';
const OPENSOURCE_SENT_EVENT_NAME = 'opensourceSent';
const ROUND_VOTE_EVENT_NAME = 'roundVotes';
const G_DONATION_EVENT_NAMES = new Set(['streamed', ROUND_STREAM_EVENT_NAME, OPENSOURCE_STREAM_EVENT_NAME, OPENSOURCE_SENT_EVENT_NAME]);
const ROUND_SUBGRAPH_URL = 'https://celo-mainnet.subgraph.x.superfluid.dev/';
const FLOW_COUNCIL_SUBGRAPH_URL =
	'https://api.goldsky.com/api/public/project_cmbkdj2bd7cr601uwafoe4u3y/subgraphs/flow-council-celo/v0.4.2/gn';
const ROUND_STREAMS_PAGE_SIZE = 1000;
const ROUND_BALLOTS_PAGE_SIZE = 1000;
const TWO_WEEKS_SECONDS = 14 * 24 * 60 * 60;
const ADDRESS_RATE_LIMIT_SECONDS = 60 * 60;

let globalEnv: { [key: string]: string };

const pointsClient = createClient<paths>({ baseUrl: 'https://cms.superfluid.pro' });
// API key passed per-request for push operations

const getCampaignIdForEvent = (eventName: string): number => {
	const defaultCampaignId = Number(globalEnv.STACK_POINT_SYSTEM_ID);
	if (Number.isNaN(defaultCampaignId)) {
		throw new Error('STACK_POINT_SYSTEM_ID is missing or invalid');
	}

	if (!G_DONATION_EVENT_NAMES.has(eventName)) {
		return defaultCampaignId;
	}

	const gdCampaignIdRaw = globalEnv.STACK_G_DONATION_POINT_SYSTEM_ID;
	if (!gdCampaignIdRaw) {
		return defaultCampaignId;
	}

	const gdCampaignId = Number(gdCampaignIdRaw);
	if (Number.isNaN(gdCampaignId)) {
		throw new Error('STACK_G_DONATION_POINT_SYSTEM_ID is invalid');
	}

	return gdCampaignId;
};

const getCampaignApiKeyForEvent = (eventName: string): string => {
	const defaultApiKey = globalEnv.STACK_KEY;
	if (!defaultApiKey) {
		throw new Error('STACK_KEY is missing');
	}

	if (!G_DONATION_EVENT_NAMES.has(eventName)) {
		return defaultApiKey;
	}

	return globalEnv.STACK_G_DONATION_KEY || defaultApiKey;
};

const getRateLimitWindowSeconds = (): number => {
	const configured = Number(globalEnv.ADDRESS_RATE_LIMIT_SECONDS || ADDRESS_RATE_LIMIT_SECONDS);
	if (!Number.isFinite(configured) || configured <= 0) {
		return ADDRESS_RATE_LIMIT_SECONDS;
	}
	return Math.floor(configured);
};

const getRateLimitCacheKey = (address: string): Request => {
	return new Request(`https://superfluid-airdrop-rate-limit/${address.toLowerCase()}`);
};

const isAddressRateLimited = async (address: string, ctx: ExecutionContext): Promise<boolean> => {
	const cache = caches.default;
	const key = getRateLimitCacheKey(address);
	const existing = await cache.match(key);
	if (existing) {
		return true;
	}

	const windowSeconds = getRateLimitWindowSeconds();
	ctx.waitUntil(
		cache.put(
			key,
			new Response('1', {
				headers: {
					'Cache-Control': `public, max-age=${windowSeconds}`,
				},
			}),
		),
	);

	return false;
};

function wait(ms: number): Promise<void> {
	return new Promise((resolve) => setTimeout(resolve, ms));
}

export function retry<T>(
	fn: () => Promise<T>,
	{ n, waitMillis }: { n: number; waitMillis: number },
): { promise: Promise<T>; cancel: () => void } {
	let completed = false;
	let rejectCancelled: (error: Error) => void;
	const promise = new Promise<T>(async (resolve, reject) => {
		rejectCancelled = reject;
		while (true) {
			let result: T;
			try {
				result = await fn();
				if (!completed) {
					resolve(result);
					completed = true;
				}
				break;
			} catch (error) {
				if (completed) {
					break;
				}
				if (n <= 0) {
					reject(error);
					completed = true;
					break;
				}
				n--;
			}
			await wait(waitMillis);
		}
	});
	return {
		promise,
		cancel: () => {
			if (completed) return;
			completed = true;
			rejectCancelled(new Error('canceled'));
		},
	};
}
export const fallback = async (asyncFns: any) => {
	if (asyncFns.length < 2) {
		// if no function passed - return undefined
		// if one function passed - immediately return its value
		// because reducer will return fn itself without invocation
		// passiing Promise.resolve as initial accumulator won't help
		// as we're reducing fns only in .catch
		return ((first(asyncFns) as () => any) || noop)();
	}

	return asyncFns.reduce(async (current: any, next: any) => {
		let promise = current;

		if (isFunction(current)) {
			promise = current();
		}

		// eslint-disable-next-line require-await
		return promise.catch(async () => {
			return next();
		});
	});
};

const getHeaders = () => {
	return new Headers([
		['Content-Type', 'application/json'],
		['Access-Control-Allow-Origin', '*'],
		// ['Access-Control-Allow-Methods', 'GET, HEAD, POST, OPTIONS'],
		// ['Access-Control-Allow-Headers', 'Content-Type, Authorization'],
		// ['Access-Control-Allow-Credentials', 'true'],
	]);
};

const parseReceivers = (value: string | undefined): string[] => {
	if (!value) {
		return [];
	}

	const receivers = value
		.split(',')
		.map((receiver) => receiver.trim().toLowerCase())
		.filter((receiver) => receiver.length > 0);

	return [...new Set(receivers)];
};

export const getExplorerEvents = async (address: string, query: any): Promise<Array<any>> => {
	const networkExplorerUrls = 'https://celo.blockscout.com/api';

	const params = {
		module: 'logs',
		action: 'getLogs',
		address,
		sort: 'asc',
		page: 1,
		offset: 1000,
		...query,
		apikey: globalEnv.CELOSCAN_KEY,
	};

	const calls = networkExplorerUrls.split(',').map((networkExplorerUrl) => {
		const url = new URL(networkExplorerUrl);
		Object.entries(params).forEach(([k, v]) => {
			url.searchParams.set(k, v as string);
		});
		const urlString = url.toString();
		return () => {
			return fetch(urlString)
				.then((result) => result.json())
				.then((result: any) => {
					if (isArray(result.result)) {
						return result.result;
					}
					console.warn('getExplorerEvents fetch failed:', result);
					throw new Error(`NOTOK ${JSON.stringify(result)}`);
				})
				.catch((e) => {
					console.warn('getExplorerEvents fetch failed:', e.message, e, urlString);
					throw e;
				});
		};
	});
	return retry(() => fallback(calls) as any, { n: 3, waitMillis: 500 }).promise as any;
};

const topWallet = async (address: string, clientIp: string) => {
	try {
		const response = await fetch(globalEnv.GOODSERVER_URL, {
			method: 'POST',
			headers: { 'content-type': 'application/json', 'X-Forwarded-For': clientIp },
			body: JSON.stringify({ chainId: 8453, account: address }),
		});
		if (response.status != 200) {
			const error = await response.text();
			console.error('topWallet failed', error);
			return { ok: 0 };
			// throw new Error(`topWallet failed: ${error}`);
		}
		const result = await response.json();
		return result;
	} catch (e: any) {
		console.error('topWallet failed:', e.message, e);
		return { ok: 0 };
	}
};

const pushPointsDelta = async ({ address, eventName, points }: { address: string; eventName: string; points: number }) => {
	if (points === 0) {
		return;
	}
	const campaignId = getCampaignIdForEvent(eventName);
	const campaignApiKey = getCampaignApiKeyForEvent(eventName);
	const result = await pointsClient.POST('/points/push', {
		headers: { 'X-API-Key': campaignApiKey },
		body: {
			campaign: campaignId,
			eventName,
			account: address,
			points,
		},
	});
	if (result.error) {
		throw new Error(`stack.so track failed (${eventName}): ${JSON.stringify(result.error)}`);
	}
};

const getEventBalance = async (address: string, eventName: string): Promise<number> => {
	const campaignId = getCampaignIdForEvent(eventName);
	const campaignApiKey = getCampaignApiKeyForEvent(eventName);
	const { data } = await pointsClient.GET('/points/event-balance', {
		headers: { 'X-API-Key': campaignApiKey },
		params: {
			query: {
				campaignId,
				account: address,
				eventName,
			},
		},
	});
	return Number(data?.points || 0);
};

const getStreamedToReceiverWei = async ({
	address,
	receivers,
	token,
	logPrefix,
}: {
	address: string;
	receivers: string[];
	token: string;
	logPrefix: string;
}): Promise<bigint> => {
	const sender = address.toLowerCase();
	const normalizedReceivers = [...new Set(receivers.map((receiver) => receiver.toLowerCase()))];
	if (normalizedReceivers.length === 0) {
		return 0n;
	}
	const campaignStartTs = BigInt(Math.max(0, Number(globalEnv.FROM_TS || 0)));
	const query = `
	query StreamPeriodsPage(
	  $sender: String!
	  $receivers: [String!]!
	  $token: String!
	  $daysago: BigInt!
	  $until: BigInt!
	  $first: Int!
	  $skip: Int!
	) {
	  streamPeriods(
		where: {
		  or: [
			{ sender: $sender, receiver_in: $receivers, token: $token, stoppedAtTimestamp: null, startedAtTimestamp_lte: $until },
			{ sender: $sender, receiver_in: $receivers, token: $token, stoppedAtTimestamp_gt: $daysago, startedAtTimestamp_lte: $until }
		  ]
		}
		first: $first
		skip: $skip
	  ) {
		sender {
		  id
		}
		flowRate
		startedAtTimestamp
		stoppedAtTimestamp
		stream {
		  userData
		}
	  }
	}
	`;

	let skip = 0;
	let hasMore = true;
	let totalStreamedWei = 0n;
	const nowTs = BigInt(Math.floor(Date.now() / 1000));

	while (hasMore) {
		const result = await retry(
			() =>
				fetch(ROUND_SUBGRAPH_URL, {
					headers: { 'content-type': 'application/json' },
					method: 'POST',
					body: JSON.stringify({
						query,
						variables: {
							sender,
							receivers: normalizedReceivers,
							token,
							daysago: campaignStartTs.toString(),
							until: nowTs.toString(),
							first: ROUND_STREAMS_PAGE_SIZE,
							skip,
						},
					}),
				})
					.then((result) => result.json())
					.then((result: any) => {
						if (isArray(result.data?.streamPeriods)) {
							return result.data.streamPeriods;
						}
						throw new Error(`NOTOK ${JSON.stringify(result)}`);
					})
					.catch((e) => {
						console.warn(`${logPrefix} fetch failed:`, e.message, e);
						throw e;
					}),
			{ n: 3, waitMillis: 1000 },
		).promise;

		console.log(`${logPrefix} fetched ${result.length}`, result);
		for (const streamPeriod of result as Array<{
			sender?: { id?: string };
			flowRate: string;
			startedAtTimestamp: string;
			stoppedAtTimestamp: string | null;
		}>) {
			if (streamPeriod?.sender?.id?.toLowerCase() !== sender) {
				continue;
			}

			const flowRate = BigInt(streamPeriod.flowRate || '0');
			if (flowRate <= 0n) {
				continue;
			}

			const startedAtTs = BigInt(streamPeriod.startedAtTimestamp || '0');
			const stoppedAtTs = streamPeriod.stoppedAtTimestamp ? BigInt(streamPeriod.stoppedAtTimestamp) : nowTs;
			const effectiveStart = startedAtTs > campaignStartTs ? startedAtTs : campaignStartTs;
			const effectiveStop = stoppedAtTs < nowTs ? stoppedAtTs : nowTs;
			const streamedSeconds = effectiveStop > effectiveStart ? effectiveStop - effectiveStart : 0n;

			totalStreamedWei += flowRate * streamedSeconds;
		}

		hasMore = result.length === ROUND_STREAMS_PAGE_SIZE;
		skip += ROUND_STREAMS_PAGE_SIZE;
	}

	console.log(
		`${logPrefix} total streamed to receivers ${receivers.join(', ')}: ${formatEther(totalStreamedWei)} G$ (${totalStreamedWei.toString()} wei)`,
	);

	return totalStreamedWei;
};

const getStreamedToReceiverPoints = async ({
	address,
	receiver,
	token,
	pointsPerGdFloat,
	eventName,
	logPrefix,
}: {
	address: string;
	receiver: string;
	token: string;
	pointsPerGdFloat: number;
	eventName: string;
	logPrefix: string;
}): Promise<{ totalStreamedGd: string; awardedPoints: string }> => {
	const totalStreamedWei = await getStreamedToReceiverWei({
		address,
		receivers: [receiver],
		token,
		logPrefix,
	});

	const totalStreamedGd = formatEther(totalStreamedWei);
	const totalPoints = Math.floor(parseFloat(totalStreamedGd) * pointsPerGdFloat);
	const awardedSoFar = await getEventBalance(address, eventName);
	const diff = totalPoints - awardedSoFar;

	if (diff !== 0) {
		console.log(`updating ${eventName} points`, {
			address,
			totalStreamedWei: totalStreamedWei.toString(),
			totalPoints,
			awardedSoFar,
			diff,
		});
		await pushPointsDelta({ address, eventName, points: diff });
	}

	return {
		totalStreamedGd,
		awardedPoints: String(totalPoints),
	};
};

const getRoundSplitterStreamPoints = async (address: string): Promise<{ totalStreamedGd: string; awardedPoints: string }> => {
	try {
		const receiver = globalEnv.ROUND_SPLITTER?.toLowerCase();
		const token = globalEnv.GOODDOLLAR?.toLowerCase();
		const pointsPerGdFloat = parseFloat(globalEnv.ROUND_POINTS_PER_GD || '0');

		if (!receiver || !token) {
			console.warn('ROUND_SPLITTER or GOODDOLLAR missing, skipping round streamed points', {
				address,
				receiver,
				token,
			});
			return { totalStreamedGd: '0', awardedPoints: '0' };
		}

		return getStreamedToReceiverPoints({
			address,
			receiver,
			token,
			pointsPerGdFloat,
			eventName: ROUND_STREAM_EVENT_NAME,
			logPrefix: 'getRoundSplitterStreamPoints',
		});
	} catch (e: any) {
		console.error('getRoundSplitterStreamPoints failed', e.message, e);
		throw e;
	}
};

const getOpenSourcePoolStreamPoints = async (address: string): Promise<{ totalStreamedGd: string; awardedPoints: string }> => {
	try {
		const receivers = parseReceivers(globalEnv.OPENSOURCE_POOLS);
		const token = globalEnv.GOODDOLLAR?.toLowerCase();
		const pointsPerGdFloat = parseFloat(globalEnv.OPENSOURCE_STREAM_POINTS_PER_GD || '0');

		if (receivers.length === 0 || !token) {
			console.warn('OPENSOURCE_POOLS or GOODDOLLAR missing, skipping opensource pool streamed points', {
				address,
				receivers,
				token,
			});
			return { totalStreamedGd: '0', awardedPoints: '0' };
		}

		const totalStreamedWei = await getStreamedToReceiverWei({
			address,
			receivers,
			token,
			logPrefix: 'getOpenSourcePoolStreamPoints',
		});

		const totalStreamedGd = formatEther(totalStreamedWei);
		const totalPoints = Math.floor(parseFloat(totalStreamedGd) * pointsPerGdFloat);
		const awardedSoFar = await getEventBalance(address, OPENSOURCE_STREAM_EVENT_NAME);
		const diff = totalPoints - awardedSoFar;

		if (diff !== 0) {
			console.log('updating opensource streamed points', {
				address,
				receivers,
				totalStreamedWei: totalStreamedWei.toString(),
				totalPoints,
				awardedSoFar,
				diff,
			});
			await pushPointsDelta({ address, eventName: OPENSOURCE_STREAM_EVENT_NAME, points: diff });
		}

		return {
			totalStreamedGd,
			awardedPoints: String(totalPoints),
		};
	} catch (e: any) {
		console.error('getOpenSourcePoolStreamPoints failed', e.message, e);
		throw e;
	}
};

const getOpenSourceSentPoints = async (address: string): Promise<{ totalSentGd: string; awardedPoints: string }> => {
	try {
		const tokenAddress = globalEnv.GOODDOLLAR;
		const receivers = parseReceivers(globalEnv.OPENSOURCE_POOLS);
		const pointsPerGdFloat = parseFloat(globalEnv.OPENSOURCE_SENT_POINTS_PER_GD || '0');
		if (!tokenAddress || receivers.length === 0) {
			console.warn('GOODDOLLAR or OPENSOURCE_POOLS missing, skipping opensource sent points', {
				address,
				tokenAddress,
				receivers,
			});
			return { totalSentGd: '0', awardedPoints: '0' };
		}

		let totalSentWei = 0n;
		for (const receiver of receivers) {
			const query = {
				address: tokenAddress,
				topic0: ERC20_TRANSFER_TOPIC,
				topic0_1_opr: 'and',
				topic0_2_opr: 'and',
				topic1_2_opr: 'and',
				topic1: padHex(address as `0x${string}`, { dir: 'left', size: 32 }).toLowerCase(),
				topic2: padHex(receiver as `0x${string}`, { dir: 'left', size: 32 }).toLowerCase(),
				fromBlock: globalEnv.FROM_BLOCK || 20506082,
				toBlock: 'latest',
				offset: 1000,
			};
			const events = await getExplorerEvents(tokenAddress, query);
			console.log('getOpenSourceSentPoints events', events, { fromBlock: globalEnv.FROM_BLOCK });
			totalSentWei += events.reduce((acc, cur) => acc + BigInt(cur.data || '0x0'), 0n);
		}
		const totalSentGd = formatEther(totalSentWei);
		const totalPoints = Math.floor(parseFloat(totalSentGd) * pointsPerGdFloat);
		const awardedSoFar = await getEventBalance(address, OPENSOURCE_SENT_EVENT_NAME);
		const diff = totalPoints - awardedSoFar;

		if (diff !== 0) {
			console.log('updating opensource sent points', {
				address,
				receivers,
				totalSentWei: totalSentWei.toString(),
				totalPoints,
				awardedSoFar,
				diff,
			});
			await pushPointsDelta({ address, eventName: OPENSOURCE_SENT_EVENT_NAME, points: diff });
		}

		return { totalSentGd, awardedPoints: String(totalPoints) };
	} catch (e: any) {
		console.error('getOpenSourceSentPoints failed', e.message, e);
		throw e;
	}
};

const getCountedRoundVotes = (timestamps: number[]): number => {
	const countedVoteTimestamps: number[] = [];
	const window: number[] = [];

	for (const ts of timestamps) {
		while (window.length > 0 && ts - window[0] >= TWO_WEEKS_SECONDS) {
			window.shift();
		}

		if (window.length < 2) {
			window.push(ts);
			countedVoteTimestamps.push(ts);
		}
	}

	return countedVoteTimestamps.length;
};

const getRoundVotesPoints = async (address: string): Promise<{ totalVotes: string; countedVotes: string; awardedPoints: string }> => {
	try {
		const roundCouncil = globalEnv.ROUND_COUNCIL?.toLowerCase();
		const pointsPerVote = Number(globalEnv.VOTE_POINTS || 0);
		if (!roundCouncil) {
			console.warn('ROUND_COUNCIL missing, skipping round votes points', { address, roundCouncil });
			return { totalVotes: '0', countedVotes: '0', awardedPoints: '0' };
		}

		const subgraphUrl = globalEnv.FLOW_COUNCIL_SUBGRAPH_URL || FLOW_COUNCIL_SUBGRAPH_URL;
		const voter = `${roundCouncil}-${address.toLowerCase()}`;
		const query = `
		query RoundVotesQuery($voter: String!, $first: Int!, $skip: Int!) {
		  ballots(where: { voter: $voter }, first: $first, skip: $skip, orderBy: createdAtTimestamp, orderDirection: asc) {
			id
			createdAtTimestamp
		  }
		}
		`;

		let skip = 0;
		let hasMore = true;
		const timestamps: number[] = [];

		while (hasMore) {
			const ballots = await retry(
				() =>
					fetch(subgraphUrl, {
						headers: { 'content-type': 'application/json' },
						method: 'POST',
						body: JSON.stringify({
							query,
							variables: {
								voter,
								first: ROUND_BALLOTS_PAGE_SIZE,
								skip,
							},
						}),
					})
						.then((result) => result.json())
						.then((result: any) => {
							if (isArray(result.data?.ballots)) {
								return result.data.ballots;
							}
							throw new Error(`NOTOK ${JSON.stringify(result)}`);
						})
						.catch((e) => {
							console.warn('getRoundVotesPoints fetch failed:', e.message, e);
							throw e;
						}),
				{ n: 3, waitMillis: 1000 },
			).promise;

			for (const ballot of ballots as Array<{ createdAtTimestamp: string }>) {
				timestamps.push(Number(ballot.createdAtTimestamp || '0'));
			}

			hasMore = ballots.length === ROUND_BALLOTS_PAGE_SIZE;
			skip += ROUND_BALLOTS_PAGE_SIZE;
		}

		const countedVotes = getCountedRoundVotes(timestamps.sort((a, b) => a - b));
		const totalPoints = countedVotes * pointsPerVote;
		const awardedSoFar = await getEventBalance(address, ROUND_VOTE_EVENT_NAME);
		const diff = totalPoints - awardedSoFar;

		if (diff !== 0) {
			console.log('updating round votes points', {
				address,
				voter,
				totalVotes: timestamps.length,
				countedVotes,
				totalPoints,
				awardedSoFar,
				diff,
			});
			await pushPointsDelta({ address, eventName: ROUND_VOTE_EVENT_NAME, points: diff });
		}

		return {
			totalVotes: String(timestamps.length),
			countedVotes: String(countedVotes),
			awardedPoints: String(totalPoints),
		};
	} catch (e: any) {
		console.error('getRoundVotesPoints failed', e.message, e);
		throw e;
	}
};

const getGoodCollectiveStreams = async (address: string): Promise<string> => {
	const subgraphUrl = globalEnv.SUBGRAPH_URL;
	const query = `
	{
    	supportEvents(where: {timestamp_gte: ${
				globalEnv.FROM_TS
			} isFlowUpdate: true donor:"${address.toLowerCase()}"} orderBy:timestamp orderDirection:asc first:1000) {
    		id  
			timestamp
			collective{
				id
			}
			donor {
				id
			}
			isFlowUpdate
			previousFlowRate
			previousContribution
			contribution
			flowRate
		}  
	}	
	`;

	try {
		const result = await retry(
			() =>
				fetch(subgraphUrl, {
					headers: { 'content-type': 'application/json', Authorization: `Bearer ${globalEnv.SUBGRAPH_KEY}` },
					method: 'POST',
					body: JSON.stringify({ query }),
				})
					.then((result) => result.json())
					.then((result: any) => {
						console.log('getGoodCollectiveStreams result:', result, subgraphUrl, address);
						if (isArray(result.data?.supportEvents)) {
							return result.data.supportEvents;
						}
						throw new Error(`NOTOK ${JSON.stringify(result)}`);
					})
					.catch((e) => {
						console.warn('getGoodCollectiveStreams fetch failed:', e.message, e);
						throw e;
					}),
			{ n: 3, waitMillis: 1000 },
		).promise;

		console.log('getGoodCollectiveStreams result:', result.length);
		// if (result.length === 0) {
		// 	return '0';
		// }

		const streamsByCollective = groupBy(result, 'collective.id');
		// console.log({ streamsByCollective });
		const streams = Object.entries(streamsByCollective)
			.map(([id, events]) => {
				console.log('pool:', id, ' events:', events.length);
				const streams = events
					.map((cur, idx) => {
						const streamSeconds = BigInt(cur.timestamp - (events?.[idx - 1]?.timestamp || 0));
						const streamRate = (BigInt(cur.contribution) - BigInt(cur.previousContribution)) / streamSeconds;
						return [streamRate, streamSeconds];
					})
					.filter((_) => _[0] > 0 && _[1] > 0);

				const lastStream = last(events);
				if (BigInt(lastStream?.flowRate) > 0) {
					const streamSeconds = BigInt(Math.floor(Date.now() / 1000) - Number(lastStream.timestamp));
					streams.push([BigInt(lastStream.flowRate), streamSeconds]);
				}
				return streams;
			})
			.flat();

		const totalStreamed = streams.reduce((acc, cur) => acc + cur[0] * cur[1], 0n);
		const totalStreamsSeconds = streams.reduce((acc, cur) => acc + cur[1], 0n);
		let totalAvgFlowRate = totalStreamed > 0 ? totalStreamed / totalStreamsSeconds : 0n;
		const maxTotalStreamed = totalAvgFlowRate > MAX_STREAM_RATE ? MAX_STREAM_RATE * totalStreamsSeconds : totalStreamed;
		const sqrdStreamed = Number(Math.sqrt(Number(maxTotalStreamed / BigInt(1e18))).toFixed(0));
		// const streamedSoFar = Number(await stack.getPoints(address, { event: 'streamed' }));
		const streamedSoFar = await getEventBalance(address, 'streamed');
		console.log(
			'fetched streams result:',
			{ address, totalStreamed: totalStreamed.toString(), sqrdStreamed, streamedSoFar },
			Object.keys(streamsByCollective),
		);
		const diff = sqrdStreamed - streamedSoFar;
		if (diff !== 0) {
			// const uniqueId = address + '_' + ((last(result) || {}) as any).timestamp + '_' + diff;
			console.log('updating stack streamed points', { address, diff, streamedSoFar });
			try {
				await pushPointsDelta({ address, eventName: 'streamed', points: diff });
				// await stack.track('streamed', { account: address, points: diff });
			} catch (e: any) {
				console.error('stack.so track failed (streamed):', e.message, e);
				throw e;
			}
		}
		return sqrdStreamed.toString();
	} catch (e: any) {
		console.error('getGoodCollectiveStreams failed', e.message, e);
		throw e;
	}
};

export const getInviteEvents = async (address: string): Promise<string> => {
	try {
		const toBlock = 'latest';
		const query = {
			address: '0x36829D1Cda92FFF5782d5d48991620664FC857d3', //invites on celo
			topic0: '0x6081787cd1bd02ab1576c52f03e8710d792d460e7881c3155d77d23893f3768b', //invite event topic
			topic0_1_opr: 'and',
			topic1: padHex(address as `0x${string}`, { dir: 'left', size: 32 }).toLowerCase(),
			fromBlock: globalEnv.FROM_BLOCK_INVITES || 20506082,
			toBlock,
			offset: 1000,
		};
		const events = await getExplorerEvents(address, query);
		// if (events.length === 0) {
		// 	return '0';
		// }
		// const invitesSoFar = Number(await pointsClient.getPoints(address, { event: 'validInvites' }));
		const invitesSoFar = await getEventBalance(address, 'validInvites');
		console.log('fetched wallet invite events:', { events: events.length, address, invitesSoFar });
		const diff = Number(globalEnv.INVITE_POINTS || 0) * events.length - invitesSoFar; // 5 points per invite
		if (diff !== 0) {
			// const uniqueId = address + '_' + last(events).timeStamp + '_' + diff;
			console.log('updating stack invites points', { address, diff, invitesSoFar });
			try {
				await pushPointsDelta({ address, eventName: 'validInvites', points: diff });
				// await stack.track('validInvites', { account: address, points: diff });
			} catch (e: any) {
				console.error('stack.so track failed (invites):', e.message, e);
				throw e;
			}
		}
		return String(events.length);
	} catch (e: any) {
		console.error('getInvites failed:', e.message, e);
		throw e;
	}
};

const getClaims = async (address: string): Promise<string> => {
	try {
		const toBlock = 'latest';
		const query = {
			address: '0x43d72Ff17701B2DA814620735C39C620Ce0ea4A1', //ubischeme on celo
			topic0: '0x89ed24731df6b066e4c5186901fffdba18cd9a10f07494aff900bdee260d1304', //claim event topic
			topic0_1_opr: 'and',
			topic1: padHex(address as `0x${string}`, { dir: 'left', size: 32 }).toLowerCase(),
			fromBlock: globalEnv.FROM_BLOCK || 20506082,
			toBlock,
			offset: 1000,
		};
		const events = await getExplorerEvents(address, query);
		// if (events.length === 0) {
		// 	return '0';
		// }
		console.log('got claims	 events:', { address, events: events.length });
		// const claimsSoFar = Number(
		// 	await stack.getPoints(address, { event: 'claimed' }).catch((e) => {
		// 		console.error('stack.so getPoints failed (claimed):', e.message, e);
		// 		throw e;
		// 	})
		// );
		const claimsSoFar = await getEventBalance(address, 'claimed');
		console.log('fetched wallet claim events:', { events: events.length, address, claimsSoFar });
		const diff = events.length * Number(globalEnv.CLAIM_POINTS || 0) - claimsSoFar;
		if (diff !== 0) {
			// const uniqueId = address + '_' + last(events).timeStamp + '_' + diff;
			console.log('updating stack claimed points', { address, diff, claimsSoFar });
			try {
				await pushPointsDelta({ address, eventName: 'claimed', points: diff });
			} catch (e: any) {
				console.error('stack.so track failed (claimed):', e.message, e);
				throw e;
			}
		}
		return String(events.length);
	} catch (e: any) {
		console.error('getClaims failed:' + e.message, e.message, e);
		throw e;
	}
};

const fetchWalletData = async (
	address: string,
): Promise<{
	claims: string;
	invites: string;
	roundStreamedToSplitter: string;
	roundStreamedPoints: string;
	opensourceStreamedToPool: string;
	opensourceStreamedPoints: string;
	opensourceSentGd: string;
	opensourceSentPoints: string;
	roundVotes: string;
	roundCountedVotes: string;
	roundVotesPoints: string;
}> => {
	const [claims, invites, roundStreamed, opensourceStreamed, opensourceSent, roundVotes] = await Promise.all([
		getClaims(address),
		getInviteEvents(address),
		getRoundSplitterStreamPoints(address),
		getOpenSourcePoolStreamPoints(address),
		getOpenSourceSentPoints(address),
		getRoundVotesPoints(address),
	]);
	return {
		claims,
		invites,
		roundStreamedToSplitter: roundStreamed.totalStreamedGd,
		roundStreamedPoints: roundStreamed.awardedPoints,
		opensourceStreamedToPool: opensourceStreamed.totalStreamedGd,
		opensourceStreamedPoints: opensourceStreamed.awardedPoints,
		opensourceSentGd: opensourceSent.totalSentGd,
		opensourceSentPoints: opensourceSent.awardedPoints,
		roundVotes: roundVotes.totalVotes,
		roundCountedVotes: roundVotes.countedVotes,
		roundVotesPoints: roundVotes.awardedPoints,
	};
};
const verifyWhitelisted = async (address: `0x${string}`): Promise<boolean> => {
	const client = createPublicClient({
		chain: celo,
		transport: http(globalEnv.CELO_RPC),
	});
	const abi = parseAbi(['function getWhitelistedRoot(address) view returns (address)']);
	const identity = getContract({ abi, address: globalEnv.IDENTITY as any, client });
	const whitelistedRoot = await identity.read.getWhitelistedRoot([address]);

	return whitelistedRoot.toLowerCase() === address.toLowerCase();
};
export default {
	async fetch(request, env, ctx): Promise<Response> {
		if (request.method != 'GET') throw new Error('unsupported request type');
		globalEnv = env as any;
		const clientIp = request.headers.get('CF-Connecting-IP');
		let url = new URL(request.url);
		const address = getAddress(url.searchParams.get('address') as any);
		console.log('incoming request:', address, clientIp);
		if (!address) {
			throw new Error('missing wallet address');
		}

		if (await isAddressRateLimited(address, ctx)) {
			const headers = getHeaders();
			headers.set('Retry-After', String(getRateLimitWindowSeconds()));
			return new Response(
				JSON.stringify({
					error: 'rate_limit_exceeded',
					message: 'Only 1 request per hour is allowed per address',
				}),
				{ headers, status: 429 },
			);
		}
		try {
			// stack = new StackClient({
			// 	// Your API key
			// 	apiKey: globalEnv.STACK_KEY,
			// 	pointSystemId: Number(globalEnv.STACK_POINT_SYSTEM_ID),
			// });
			const isWhitelisted = await verifyWhitelisted(address as any);
			const [topWalletResult, walletData] = await Promise.all([
				isWhitelisted ? topWallet(address, clientIp || '') : Promise.resolve({ result: 'not_whitelisted' }),
				fetchWalletData(address),
			]);

			console.log('results:', { address, clientIp, topWalletResult, walletData });
			return new Response(
				JSON.stringify({
					topWalletResult,
					walletData,
				}),
				{ headers: getHeaders(), status: 200 },
			);
		} catch (e: any) {
			console.error('superfluid request failed', { address, error: e.message, e, globalEnv });
			throw e;
		}
	},
} satisfies ExportedHandler<Env>;
