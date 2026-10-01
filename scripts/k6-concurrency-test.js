import http from 'k6/http';
import { check, fail } from 'k6';
import { Counter } from 'k6/metrics';

const scenario = __ENV.SCENARIO || 'duplicate';
const duplicateCount = Number(__ENV.ITERATIONS || 50);
const virtualUsers = Number(__ENV.VUS || (scenario === 'duplicate' ? 50 : 10));
const processed = new Counter('wager_processed');
const rejected = new Counter('wager_rejected');
const fresh = new Counter('idempotency_fresh');
const replay = new Counter('idempotency_replay');

const thresholds = {};
if (scenario === 'duplicate') {
  thresholds.idempotency_fresh = ['count==1'];
  thresholds.idempotency_replay = [`count==${duplicateCount - 1}`];
} else if (scenario === 'compete') {
  thresholds.wager_processed = ['count==1'];
  thresholds.wager_rejected = ['count==1'];
}

export const options = {
  scenarios: {
    wagering: scenario === 'duplicate'
      ? { executor: 'shared-iterations', vus: virtualUsers, iterations: duplicateCount, maxDuration: '2m' }
      : { executor: 'per-vu-iterations', vus: scenario === 'compete' ? 2 : virtualUsers, iterations: 1, maxDuration: '2m' },
  },
  thresholds,
  summaryTrendStats: ['avg', 'min', 'med', 'max', 'p(90)', 'p(95)', 'p(99)'],
};

function required(name) {
  const value = __ENV[name];
  if (!value) {
    fail(`missing required environment variable ${name}`);
  }
  return value;
}

function token(issuer, clientID, clientSecret) {
  const response = http.post(`${issuer}/protocol/openid-connect/token`, {
    grant_type: 'client_credentials',
    client_id: clientID,
    client_secret: clientSecret,
  }, { headers: { 'Content-Type': 'application/x-www-form-urlencoded' } });
  const valid = check(response, { 'OIDC token acquired': (result) => result.status === 200 });
  if (!valid) {
    fail(`OIDC token request failed with HTTP ${response.status}`);
  }
  return response.json('access_token');
}

function openWallet(api, accessToken, playerID, amount) {
  const response = http.post(`${api}/wallets`, JSON.stringify({
    playerId: playerID,
    initialBalance: { amount, currency: 'BRL' },
  }), {
    headers: { Authorization: `Bearer ${accessToken}`, 'Content-Type': 'application/json' },
  });
  if (!check(response, { 'wallet opened': (result) => result.status === 201 })) {
    fail(`wallet creation failed with HTTP ${response.status}`);
  }
  return response.json('id');
}

export function setup() {
  if (!['duplicate', 'compete', 'parallel'].includes(scenario)) {
    fail(`unsupported SCENARIO ${scenario}`);
  }
  const api = required('API_BASE_URL').replace(/\/$/, '');
  const issuer = required('OIDC_ISSUER_URL').replace(/\/$/, '');
  const runID = __ENV.RUN_ID || `k6-${Date.now()}`;
  const providerToken = token(issuer, required('OIDC_PROVIDER_CLIENT_ID'), required('OIDC_PROVIDER_CLIENT_SECRET'));
  const internalToken = token(issuer, required('OIDC_INTERNAL_CLIENT_ID'), required('OIDC_INTERNAL_CLIENT_SECRET'));
  const providerID = __ENV.PROVIDER_ID || 'provider-alpha';
  const walletCount = scenario === 'parallel' ? virtualUsers : 1;
  const wallets = [];
  for (let index = 0; index < walletCount; index += 1) {
    const playerID = `${runID}-player-${index}`;
    wallets.push({ id: openWallet(api, internalToken, playerID, '100.00'), playerID });
  }
  return { api, providerToken, internalToken, providerID, runID, wallets };
}

function submitWager(data, wallet, externalID, idempotencyKey, amount) {
  const response = http.post(`${data.api}/wagering/transactions`, JSON.stringify({
    providerId: data.providerID,
    externalTransactionId: externalID,
    playerId: wallet.playerID,
    walletId: wallet.id,
    roundId: `${data.runID}-round`,
    gameId: 'k6-concurrency-test',
    kind: 'BET',
    money: { amount, currency: 'BRL' },
  }), {
    headers: {
      Authorization: `Bearer ${data.providerToken}`,
      'Content-Type': 'application/json',
      'Idempotency-Key': idempotencyKey,
    },
  });
  let body;
  try {
    body = response.json();
  } catch (_) {
    body = {};
  }
  if (body.status === 'PROCESSED') {
    processed.add(1);
  } else if (body.status === 'REJECTED') {
    rejected.add(1);
  }
  if (body.idempotentReplay === true) {
    replay.add(1);
  } else if (body.idempotentReplay === false) {
    fresh.add(1);
  }
  return { response, body };
}

export default function (data) {
  if (scenario === 'duplicate') {
    const result = submitWager(data, data.wallets[0], `${data.runID}-same-bet`, `${data.runID}-same-key`, '25.00');
    check(result, {
      'duplicate operation processed': (value) => value.response.status === 200 && value.body.status === 'PROCESSED',
      'duplicate operation replay flag present': (value) => typeof value.body.idempotentReplay === 'boolean',
    });
    return;
  }

  if (scenario === 'compete') {
    const suffix = __VU === 1 ? 'bet-a' : 'bet-b';
    const result = submitWager(data, data.wallets[0], `${data.runID}-${suffix}`, `${data.runID}-${suffix}-key`, '80.00');
    check(result, {
      'competing wager has terminal expected response': (value) =>
        (value.response.status === 200 && value.body.status === 'PROCESSED') ||
        (value.response.status === 422 && value.body.status === 'REJECTED' && value.body.failureCode === 'INSUFFICIENT_BALANCE'),
    });
    return;
  }

  const walletIndex = __VU - 1;
  const result = submitWager(data, data.wallets[walletIndex], `${data.runID}-parallel-${walletIndex}`, `${data.runID}-parallel-key-${walletIndex}`, '1.00');
  check(result, { 'independent wallet wager processed': (value) => value.response.status === 200 && value.body.status === 'PROCESSED' });
}

export function teardown(data) {
  const expectedBalance = scenario === 'duplicate' ? '75.00' : scenario === 'compete' ? '20.00' : '99.00';
  for (const walletID of data.wallets) {
    const headers = { Authorization: `Bearer ${data.internalToken}` };
    const wallet = http.get(`${data.api}/wallets/${walletID.id}`, { headers });
    const ledger = http.get(`${data.api}/wallets/${walletID.id}/ledger?limit=100`, { headers });
    const reconciliation = http.post(`${data.api}/wallets/${walletID.id}/reconciliation`, '{}', {
      headers: { ...headers, 'Content-Type': 'application/json' },
    });
    check({ wallet, ledger, reconciliation }, {
      'wallet balance matches scenario': (result) => result.wallet.status === 200 && result.wallet.json('balance.amount') === expectedBalance,
      'ledger contains opening and one debit': (result) => result.ledger.status === 200 && result.ledger.json('entries').length === 2,
      'wallet reconciles against ledger': (result) => result.reconciliation.status === 200 && result.reconciliation.json('consistent') === true,
    });
  }
}