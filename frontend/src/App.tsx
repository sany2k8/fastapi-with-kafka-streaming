import { useCallback, useEffect, useState } from "react";

import { api, type KafkaInspect, type Payment } from "./api";

/**
 * The whole point of this page: submit a payment, watch the row appear as
 * `processing`, and watch it flip to `approved` or `blocked` a moment later
 * without you doing anything. That gap is the asynchronous pipeline.
 */
export default function App() {
  const [payments, setPayments] = useState<Payment[]>([]);
  const [kafka, setKafka] = useState<KafkaInspect | null>(null);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const [userId, setUserId] = useState("user-123");
  const [amount, setAmount] = useState("50");
  const [country, setCountry] = useState("US");
  const [deviceId, setDeviceId] = useState("device-456");

  const refresh = useCallback(async () => {
    try {
      const [rows, inspect] = await Promise.all([api.listPayments(30), api.inspect()]);
      setPayments(rows);
      setKafka(inspect);
      setError(null);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    }
  }, []);

  // Polling, not websockets: one less moving part, and it makes the
  // processing -> decided transition obvious on screen.
  useEffect(() => {
    void refresh();
    const timer = setInterval(() => void refresh(), 1000);
    return () => clearInterval(timer);
  }, [refresh]);

  async function submit(event: React.FormEvent) {
    event.preventDefault();
    setBusy(true);
    try {
      await api.createPayment({
        user_id: userId,
        amount: Number(amount),
        currency: "USD",
        country,
        device_id: deviceId,
      });
      await refresh();
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(false);
    }
  }

  /** Reproduces scripts/burst.sh from the browser. */
  async function runTakeover() {
    setBusy(true);
    const victim = `user-ui-${Math.floor(Math.random() * 10000)}`;
    try {
      for (let i = 1; i <= 25; i++) {
        const takeover = i > 19;
        await api.createPayment({
          user_id: victim,
          amount: i * 10,
          currency: "USD",
          country: takeover ? (i % 2 === 0 ? "BD" : "US") : "US",
          device_id: takeover ? `device-stolen-${i}` : "device-known",
        });
      }
      await refresh();
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(false);
    }
  }

  return (
    <main>
      <header>
        <h1>Real-time fraud detection</h1>
        <p>
          POST /payments returns <code>processing</code> immediately. Scoring happens in a Kafka
          consumer, so rows below change status on their own.
        </p>
      </header>

      {error && <div className="error">API error: {error}</div>}

      <section className="panels">
        <form onSubmit={submit} className="card">
          <h2>Make a payment</h2>
          <label>
            User <input value={userId} onChange={(e) => setUserId(e.target.value)} />
          </label>
          <label>
            Amount <input value={amount} onChange={(e) => setAmount(e.target.value)} type="number" />
          </label>
          <label>
            Country
            <select value={country} onChange={(e) => setCountry(e.target.value)}>
              <option>US</option>
              <option>BD</option>
              <option>GB</option>
            </select>
          </label>
          <label>
            Device <input value={deviceId} onChange={(e) => setDeviceId(e.target.value)} />
          </label>
          <div className="buttons">
            <button type="submit" disabled={busy}>
              Submit payment
            </button>
            <button type="button" onClick={runTakeover} disabled={busy} className="danger">
              Simulate takeover (25)
            </button>
          </div>
          <p className="hint">
            Amount over 5000 scores +20. A country or device the user has not used before scores
            +30 / +10. Twenty payments inside 30s scores +40. At 70 the payment is blocked.
          </p>
        </form>

        <div className="card">
          <h2>Kafka</h2>
          {kafka ? (
            <>
              {Object.entries(kafka.topics).map(([topic, info]) => (
                <div key={topic} className="topic">
                  <strong>{topic}</strong>
                  <span className="muted"> {info.partition_count} partitions</span>
                  <div className="partitions">
                    {info.partitions.map((p) => (
                      <span key={p.partition} className="pill" title="end offset">
                        p{p.partition}: {p.end_offset}
                      </span>
                    ))}
                  </div>
                </div>
              ))}
              <table className="groups">
                <thead>
                  <tr>
                    <th>consumer group</th>
                    <th>members</th>
                    <th>lag</th>
                  </tr>
                </thead>
                <tbody>
                  {Object.entries(kafka.consumer_groups).map(([group, info]) => (
                    <tr key={group}>
                      <td>{group}</td>
                      <td>{info.member_count}</td>
                      <td className={info.total_lag > 0 ? "lag" : ""}>{info.total_lag}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
              <p className="hint">
                Two groups read <code>fraud.events</code> and both see every record - that is
                fan-out, not a queue.
              </p>
            </>
          ) : (
            <p className="muted">loading…</p>
          )}
        </div>
      </section>

      <section className="card">
        <h2>Recent payments</h2>
        <div className="scroll">
          <table className="payments">
            <thead>
              <tr>
                <th>payment</th>
                <th>user</th>
                <th>amount</th>
                <th>country</th>
                <th>device</th>
                <th>status</th>
                <th>score</th>
                <th>level</th>
              </tr>
            </thead>
            <tbody>
              {payments.map((p) => (
                <tr key={p.payment_id} className={p.status}>
                  <td className="mono">{p.payment_id}</td>
                  <td>{p.user_id}</td>
                  <td className="num">{p.amount}</td>
                  <td>{p.country}</td>
                  <td className="mono">{p.device_id}</td>
                  <td>
                    <span className={`badge ${p.status}`}>{p.status}</span>
                  </td>
                  <td className="num">{p.risk_score ?? "—"}</td>
                  <td>{p.risk_level ?? "—"}</td>
                </tr>
              ))}
              {payments.length === 0 && (
                <tr>
                  <td colSpan={8} className="muted">
                    No payments yet.
                  </td>
                </tr>
              )}
            </tbody>
          </table>
        </div>
      </section>
    </main>
  );
}
