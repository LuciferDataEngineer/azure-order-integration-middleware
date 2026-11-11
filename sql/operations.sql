-- Counts by subscription and delivery state include retries and repeated deliveries.
SELECT subscription, state, count(*) AS deliveries, sum(attempts) AS attempts
FROM deliveries GROUP BY subscription, state ORDER BY subscription, state;

-- Closed and open DLQ cases. A redriven delivery can create another case if it fails again.
SELECT subscription, coalesce(resolution,'open') AS resolution, count(*) AS cases
FROM dead_letters GROUP BY subscription, resolution ORDER BY subscription, resolution;

-- Current monetary value is additive across order IDs, not across all versions.
SELECT status, count(*) AS orders, sum(total_minor) AS current_total_minor
FROM fulfillment_state GROUP BY status ORDER BY status;

-- Diagnostic for a drained pipeline: each admitted event should have one receipt per subscriber.
WITH expected(subscription) AS (VALUES ('audit'), ('fulfillment'))
SELECT s.subscription, count(*) AS missing_receipts FROM expected s CROSS JOIN events e
LEFT JOIN receipts r ON r.event_id=e.event_id AND r.subscription=s.subscription
WHERE r.event_id IS NULL GROUP BY s.subscription ORDER BY s.subscription;
