import http from 'k6/http';
import { check, sleep } from 'k6';

const API_BASE_URL = __ENV.API_BASE_URL || 'http://localhost:8000';
const API_KEY = __ENV.API_KEY || 'wh_live_test_key';

export const options = {
  stages: [
    { duration: '10s', target: 5 },
    { duration: '20s', target: 20 },
    { duration: '10s', target: 50 },
    { duration: '10s', target: 0 },
  ],
  thresholds: {
    http_req_duration: ['p(95)<800'],
    http_req_failed: ['rate<0.02'],
  },
};

export function setup() {
  if (__ENV.DESTINATION_ID) {
    return { destinationId: __ENV.DESTINATION_ID };
  }
  const params = {
    headers: {
      'Content-Type': 'application/json',
      'Authorization': `Bearer ${API_KEY}`,
    },
  };
  const listRes = http.get(`${API_BASE_URL}/v1/destinations`, params);
  if (listRes.status === 200) {
    const parsed = JSON.parse(listRes.body);
    const destinations = Array.isArray(parsed) ? parsed : (parsed.items || []);
    const activeDest = destinations.find((d) => d.is_active && d.url.includes('/webhooks/success'));
    if (activeDest) {
      return { destinationId: activeDest.id };
    }
    if (destinations.length > 0) {
      return { destinationId: destinations[0].id };
    }
  }
  const createRes = http.post(
    `${API_BASE_URL}/v1/destinations`,
    JSON.stringify({
      name: 'k6 Ramping Receiver',
      url: 'http://test-receiver:9000/webhooks/success',
    }),
    params
  );
  if (createRes.status === 201) {
    return { destinationId: JSON.parse(createRes.body).id };
  }
  return { destinationId: '' };
}

export default function (data) {
  const payload = JSON.stringify({
    event_type: 'load_test.ramping',
    payload: {
      vu: __VU,
      iter: __ITER,
      timestamp: Date.now(),
    },
    destination_ids: [data.destinationId],
  });

  const params = {
    headers: {
      'Content-Type': 'application/json',
      'Authorization': `Bearer ${API_KEY}`,
    },
  };

  const res = http.post(`${API_BASE_URL}/v1/events`, payload, params);

  check(res, {
    'status is 202': (r) => r.status === 202,
  });

  sleep(0.05);
}
