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
import { isArray, first, isFunction, noop, groupBy, last, add } from 'lodash';
import { celo } from 'viem/chains';
import { createPublicClient, getContract, http, padHex, parseAbi, getAddress } from 'viem';
import { StackClient } from '@stackso/js-core';

const MAX_DAILY_STREAM = BigInt(73000 * 1e18); //73k G$
const MAX_STREAM_RATE = MAX_DAILY_STREAM / (24n * 60n * 60n);

let globalEnv: { [key: string]: string };
let stack: StackClient;

const client = createPublicClient({
	chain: celo,
	transport: http(),
});

function wait(ms: number): Promise<void> {
	return new Promise((resolve) => setTimeout(resolve, ms));
}

export function retry<T>(
	fn: () => Promise<T>,
	{ n, waitMillis }: { n: number; waitMillis: number }
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

export const getExplorerEvents = async (address: string, query: any): Promise<Array<any>> => {
	const networkExplorerUrls = 'https://api.celoscan.io/api,https://explorer.celo.org/mainnet/api';

	const params = { module: 'logs', action: 'getLogs', address, sort: 'asc', page: 1, offset: 1000, ...query };

	const calls = networkExplorerUrls.split(',').map((networkExplorerUrl) => {
		const url = new URL(networkExplorerUrl);
		Object.entries(params).forEach(([k, v]) => {
			url.searchParams.set(k, v as string);
		});
		return () =>
			fetch(url)
				.then((result) => result.json())
				.then((result: any) => {
					if (isArray(result.result)) {
						return result.result;
					}
					throw new Error(`NOTOK ${JSON.stringify(result)}`);
				})
				.catch((e) => {
					console.error('getExplorerEvents fetch failed:', e.message, e, url.toString());
					throw e;
				});
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
			console.warn('topWallet failed');
			throw new Error(`topWallet failed: ${error}`);
		}
		const result = await response.json();
		return result;
	} catch (e: any) {
		console.error('topWallet failed:', e.message, e);
		return { ok: 0 };
	}
};
const getGoodCollectiveStreams = async (address: string): Promise<string> => {
	const subgraphUrl = globalEnv.SUBGRAPH_URL;
	const query = `
	{
    	supportEvents(where: {isFlowUpdate: true donor:"${address.toLowerCase()}"} orderBy:timestamp orderDirection:asc) {
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
					headers: { 'content-type': 'application/json' },
					method: 'POST',
					body: JSON.stringify({ query }),
				})
					.then((result) => result.json())
					.then((result: any) => {
						console.log(result.data, subgraphUrl, address);
						if (isArray(result.data.supportEvents)) {
							return result.data.supportEvents;
						}
						throw new Error(`NOTOK ${JSON.stringify(result)}`);
					})
					.catch((e) => {
						console.error('getGoodCollectiveStreams fetch failed:', e.message, e);
						throw e;
					}),
			{ n: 3, waitMillis: 1000 }
		).promise;

		console.log('getGoodCollectiveStreams result:', result.length);

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
		const streamedSoFar = Number(await stack.getPoints(address, { event: 'streamed' }));
		console.log(
			'fetched streams result:',
			{ address, totalStreamed: totalStreamed.toString(), sqrdStreamed, streamedSoFar },
			Object.keys(streamsByCollective)
		);
		const diff = sqrdStreamed - streamedSoFar;
		if (diff > 0) {
			const uniqueId = address + '_' + (last(result) as any).timestamp;
			console.log('updating stack streamed points', { address, diff, streamedSoFar, uniqueId });
			await stack.track('streamed', { account: address, points: diff, uniqueId });
		}
		return sqrdStreamed.toString();
	} catch (e: any) {
		console.error('getGoodCollectiveStreams failed', e.message, e);
		throw e;
	}
};

const getClaims = async (address: string): Promise<string> => {
	try {
		const toBlock = await client.getBlockNumber();
		const query = {
			address: '0x43d72Ff17701B2DA814620735C39C620Ce0ea4A1', //ubischeme on celo
			topic0: '0x89ed24731df6b066e4c5186901fffdba18cd9a10f07494aff900bdee260d1304', //claim event topic
			topic0_1_opr: 'and',
			topic1: padHex(address as `0x${string}`, { dir: 'left', size: 32 }).toLowerCase(),
			fromBlock: globalEnv.FROM_BLOCK || 20506082,
			toBlock,
			offset: 1000,
			apikey: globalEnv.CELOSCAN_KEY,
		};
		const events = await getExplorerEvents(address, query);
		const claimsSoFar = Number(await stack.getPoints(address, { event: 'claimed' }));
		console.log('fetched wallet claim events:', { events: events.length, address, claimsSoFar });
		const diff = events.length - claimsSoFar;
		if (diff > 0) {
			const uniqueId = address + '_' + last(events).timeStamp;
			console.log('updating stack claimed points', { address, diff, claimsSoFar, uniqueId });
			await stack.track('claimed', { account: address, points: diff, uniqueId });
		}
		return String(events.length);
	} catch (e: any) {
		console.error('fetchWalletData failed:', e.message, e);
		throw e;
	}
};

const fetchWalletData = async (address: string): Promise<{ claims: string; streamed: string }> => {
	const [streamed, claims] = await Promise.all([getGoodCollectiveStreams(address), getClaims(address)]);
	return { claims, streamed };
};
const verifyWhitelisted = async (address: `0x${string}`): Promise<boolean> => {
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
		try {
			stack = new StackClient({
				// Your API key
				apiKey: globalEnv.STACK_KEY,
				pointSystemId: 7246,
			});
			const isWhitelisted = await verifyWhitelisted(address as any);
			if (isWhitelisted === false) {
				return new Response(
					JSON.stringify({
						error: 'not whitelisted',
					}),
					{ headers: getHeaders(), status: 200 }
				);
			}
			const [topWalletResult, walletData] = await Promise.all([topWallet(address, clientIp || ''), fetchWalletData(address)]);
			console.log('results:', { address, clientIp, topWalletResult, walletData });
			return new Response(
				JSON.stringify({
					topWalletResult,
					walletData,
				}),
				{ headers: getHeaders(), status: 200 }
			);
		} catch (e: any) {
			console.log('failed data fetch', { address, error: e.message, e, globalEnv });
			throw e;
		}
	},
} satisfies ExportedHandler<Env>;
