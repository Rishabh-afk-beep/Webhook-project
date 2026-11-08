import http from 'k6/http';
import { check, sleep } from 'k6';

const API_BASE_URL = __ENV.API_BASE_URL || 'http://localhost:8000';
const API_KEY = __ENV.API_KEY || 'wh_live_test_key';

export const options = {
  vus: parseInt(__ENV.VUS || '5'),
  duration: __ENV.DURATION || '20s',
};

export function setup() {
  const params = {
    headers: {
      'Content-Type': 'application/json',
      'Authorization': `Bearer ${API_KEY}`,
    },
  };
  const listRes = http.get(`${API_BASE_URL}/v1/destinations`, params);
  let destIds = [];
  if (listRes.status === 200) {
    const destinations = JSON.parse(listRes.body);
    destIds = destinations.map((d) => d.id);
  }
  while (destIds.length < 2) {
    const createRes = http.post(
      `${API_BASE_URL}/v1/destinations`,
      JSON.stringify({
        name: `k6 Broadcast Receiver ${destIds.length + 1}`,
        url: 'http://test-receiver:9000/webhooks/success',
      }),
      params
    );
    if (createRes.status === 201) {
      destIds.push(JSON.parse(createRes.body).id);
    } else {
      break;
    }
  }
  return { destinationIds: destIds };
}

export default function (data) {
  const payload = JSON.stringify({
    event_type: 'load_test.multi_destination',
    payload: { broadcast: true, batch_id: __ITER },
    destination_ids: data.destinationIds,
  });

  const res = http.post(`${API_BASE_URL}/v1/events`, payload, {
    headers: {
      'Content-Type': 'application/json',
      'Authorization': `Bearer ${API_KEY}`,
    },
  });

  check(res, {
    'accepted 202': (r) => r.status === 202,
    'multiple deliveries generated': (r) => {
      try {
        const body = JSON.parse(r.body);
        return body.delivery_count >= 1;
      } catch (e) {
        return false;
      }
    },
  });

  sleep(0.2);
}
