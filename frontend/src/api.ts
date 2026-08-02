export type Payment = {
  payment_id: string;
  user_id: string;
  amount: string;
  currency: string;
  country: string;
  device_id: string;
  status: "processing" | "approved" | "blocked";
  created_at: string;
  risk_score: number | null;
  risk_level: string | null;
};

export type NewPayment = {
  user_id: string;
  amount: number;
  currency: string;
  country: string;
  device_id: string;
};

export type KafkaGroup = {
  state: string;
  member_count: number;
  total_lag: number;
  members: { assigned_partitions: Record<string, number[]> }[];
  offsets: { topic: string; partition: number; committed_offset: number; end_offset: number; lag: number }[];
};

export type KafkaInspect = {
  topics: Record<string, { partition_count: number; total_records: number; partitions: { partition: number; end_offset: number }[] }>;
  consumer_groups: Record<string, KafkaGroup>;
};

async function json<T>(response: Response): Promise<T> {
  if (!response.ok) throw new Error(`${response.status} ${response.statusText}`);
  return (await response.json()) as T;
}

export const api = {
  listPayments: (limit = 30) =>
    fetch(`/payments?limit=${limit}`).then(json<Payment[]>),

  createPayment: (body: NewPayment) =>
    fetch("/payments", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(body),
    }).then(json<{ payment_id: string; status: string }>),

  inspect: () => fetch("/kafka/inspect").then(json<KafkaInspect>),
};
