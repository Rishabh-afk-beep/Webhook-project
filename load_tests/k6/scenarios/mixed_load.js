import http from 'k6/http';
import { check, sleep } from 'k6';

const API_BASE_URL = __ENV.API_BASE_URL || 'http://localhost:8000';
const API_KEY = __ENV.API_KEY || 'wh_live_test_key';

export const options = {
  vus: parseInt(__ENV.VUS || '10'),
  duration: __ENV.DURATION || '30s',
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
    const destinations = JSON.parse(listRes.body);
    if (destinations.length > 0) {
      return { destinationId: destinations[0].id };
    }
  }
  const createRes = http.post(
    `${API_BASE_URL}/v1/destinations`,
    JSON.stringify({
      name: 'k6 Mixed Receiver',
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
  const isInvalid = Math.random() < 0.2; // 20% invalid requests

  if (isInvalid) {
    // Bad auth request
    const res = http.get(`${API_BASE_URL}/v1/destinations`, {
      headers: { 'Authorization': 'Bearer invalid_key' },
    });
    check(res, { 'auth rejected with 401': (r) => r.status === 401 });
  } else {
    // Valid event
    const payload = JSON.stringify({
      event_type: 'load_test.mixed',
      payload: { valid: true, id: __ITER },
      destination_ids: [data.destinationId],
    });
    const res = http.post(`${API_BASE_URL}/v1/events`, payload, {
      headers: {
        'Content-Type': 'application/json',
        'Authorization': `Bearer ${API_KEY}`,
      },
    });
    check(res, { 'valid event accepted 202': (r) => r.status === 202 });
  }

  sleep(0.1);
}
